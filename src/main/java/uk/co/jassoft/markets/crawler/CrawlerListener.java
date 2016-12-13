/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.co.jassoft.markets.crawler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.co.jassoft.markets.datamodel.crawler.Link;
import uk.co.jassoft.markets.datamodel.sources.Source;
import uk.co.jassoft.markets.datamodel.sources.SourceUrl;
import uk.co.jassoft.markets.datamodel.sources.errors.SourceError;
import uk.co.jassoft.markets.datamodel.story.Story;
import uk.co.jassoft.markets.datamodel.system.Queue;
import uk.co.jassoft.markets.repository.SourceErrorRepository;
import uk.co.jassoft.markets.repository.SourceRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import uk.co.jassoft.markets.service.LinkService;
import uk.co.jassoft.markets.utils.SourceUtils;
import uk.co.jassoft.network.Network;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 *
 * @author Jonny
 */
@Component
@Service
public class CrawlerListener implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(CrawlerListener.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    private StoryRepository storyRepository;

    @Autowired
    private SourceErrorRepository sourceErrorRepository;

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private Network network;

    @Autowired
    private LinkService linkService;

    void collectStory(final String message) {
        jmsTemplate.convertAndSend(Queue.Stories.toString(), message);
    }

    void crawlLink(final Object message) throws JsonProcessingException {
        jmsTemplate.convertAndSend(Queue.Crawler.toString(), mapper.writeValueAsString(message));
    }

    @Override
    @JmsListener(destination = "Crawler", concurrency = "5")
    public void onMessage(final Message message) {
        if (message instanceof TextMessage) {
            final TextMessage textMessage = (TextMessage) message;
            try {
                message.acknowledge();

                final Story story = mapper.readValue(textMessage.getText(), Story.class);

                Source source = sourceRepository.findOne(story.getParentSource());

                if(source.isDisabled()) {
                    LOG.info("Source [{}] is Disabled", source.getName());
                    final Link link = linkService.findOneByLink(story.getUrl().toString());
                    if(link != null) {
                        linkService.delete(link);
                    }
                    return;
                }

                if(SourceUtils.matchesExclusion(source.getExclusionList(), story.getUrl().toString())) {
                    LOG.info("Story Link Matches Exclusion for Source [{}]", source.getName());

                    final Link link = linkService.findOneByLink(story.getUrl().toString());
                    if(link != null) {
                        linkService.delete(link);
                    }

                    return;
                }

                if(isbaseURL(source.getUrls(), story.getUrl().toString())) {

                    SourceUrl currentSourceUrl = source.getUrls().parallelStream().filter(sourceUrl -> sourceUrl.getUrl().equals(story.getUrl().toString())).findFirst().get();

                    if(!currentSourceUrl.isEnabled() || (currentSourceUrl.getDisabledUntil() != null && currentSourceUrl.getDisabledUntil().after(new Date()))) {
                        LOG.info("Source URL [{}] is Disabled", currentSourceUrl.getUrl());
                        final Link link = linkService.findOneByLink(story.getUrl().toString());
                        if(link != null) {
                            linkService.delete(link);
                        }
                        return;
                    }
                }

                try (InputStream inputStream = network.read(story.getUrl().toString(), "GET", !isbaseURL(source.getUrls(), story.getUrl().toString()))) {

                    Document doc = Jsoup.parse(inputStream, "UTF-8", story.getUrl().toString());

                    Elements links = doc.select("a[href]");

                    doc = null;

                    LOG.debug("Found [{}] Links in [{}]", links.size(), story.getUrl().toString());

                    AtomicInteger newLinkCount = new AtomicInteger(0);

                    links.stream().map(link -> {
                                String linkHref = link.attr("abs:href");

                                if (linkHref.contains("#"))
                                    linkHref = linkHref.substring(0, linkHref.indexOf("#"));

                                return new ImmutablePair<String, Element>(linkHref, link);

                            })
                            .filter(isNotGlobalExclusion())
                            .filter(isValidUrl(source))
                            .filter(doesNotMatchExclusion(source))
                            .filter(isNewLink(linkService, storyRepository))
                            .forEach(link -> {

                                try {
                                    LOG.debug("{} - {}", link.getKey(), link.getValue().text());

                                    Story storyFound = new Story(link.getValue().text(), new URL(link.getKey()), new Date(), story.getParentSource());

                                    crawlLink(storyFound);

                                    newLinkCount.incrementAndGet();

                                    Story existingStory = storyRepository.findOneByUrl(storyFound.getUrl().toString());

                                    if (existingStory != null)
                                        return;

                                    if(link.getKey().getBytes().length < 1000) {

                                        storyRepository.save(storyFound);

                                        collectStory(storyFound.getId());

                                        linkService.save(new Link(link.getKey()));
                                    }
                                    else {
                                        LOG.warn("Link too long to persist. Not Persisting. {} - {}", link.getKey(), link.getValue().text());
                                    }

                                } catch (final Exception exception) {
                                    LOG.error("Error found with Link {} - {}", link.getKey(), link.getValue().text() + ": " + exception.getLocalizedMessage());
                                }
                            });

                    if (newLinkCount.get() > 0) {
                        LOG.info("Found [{}] New Links in [{}]", newLinkCount.get(), story.getUrl().toString());
                    }

                    links = null;
                } catch (IOException exception) {
                    LOG.warn("IOException Crawling Link [{}] - [{}]", story.getUrl().toString(), exception.getMessage());
                    sourceErrorRepository.save(new SourceError(source.getId(), new Date(), story.getUrl().toString(), null, exception.getMessage()));
                    return;
                }

                if(isbaseURL(source.getUrls(), story.getUrl().toString())) {

                    SourceUrl currentSourceUrl = source.getUrls().parallelStream().filter(sourceUrl -> sourceUrl.getUrl().equals(story.getUrl().toString())).findFirst().get();

                    currentSourceUrl.setLastCrawled(new Date());
                    currentSourceUrl.setPendingCrawl(false);

                    if(currentSourceUrl.getCrawlInterval() == null) {
                        currentSourceUrl.setCrawlInterval(60);
                    }

                    mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(source.getId()).and("urls.url").is(story.getUrl().toString())),
                            new Update().set("urls.$", currentSourceUrl)
                            , Source.class);
                }

            } catch (IOException exception) {
                LOG.warn(exception.getLocalizedMessage());
                return;
            } catch (final Exception exception) {
                LOG.error(exception.getLocalizedMessage(), exception);

                throw new RuntimeException(exception);
            }
        }
    }



    private boolean isbaseURL(List<SourceUrl> sourceUrls, String url) {
        return sourceUrls.parallelStream().anyMatch(sourceUrl -> sourceUrl.getUrl().equals(url));
    }

    public static Predicate<Pair<String, Element>> isNotGlobalExclusion() {
        return link -> link.getKey().matches("^(http).+") && // Contains http
                !link.getKey().matches(".+.(png|jpg|jpeg)$") && // Is not an image
                !link.getKey().matches(".+(facebook.com/dialog).+") && // Isnt a facebook dialogue
                !link.getKey().matches(".+(edit.digg.com).+"); // Isnt a Digg edit page
    }

    public static Predicate<Pair<String, Element>> isValidUrl(Source source) {
        return link -> SourceUtils.isValidURL(source.getUrls(), link.getKey()) || source.isCollectDirectlyLinked();
    }

    public static Predicate<Pair<String, Element>> doesNotMatchExclusion(Source source) {
        return link -> !SourceUtils.matchesExclusion(source.getExclusionList(), link.getKey());
    }

    public static Predicate<Pair<String, Element>> isNewLink(LinkService linkService, StoryRepository storyRepository) {
        return link -> linkService.findOneByLink(link.getKey()) == null && storyRepository.findOneByUrl(link.getKey()) == null;
    }


}