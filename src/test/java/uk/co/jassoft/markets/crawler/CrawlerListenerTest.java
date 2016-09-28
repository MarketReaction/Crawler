package uk.co.jassoft.markets.crawler;

import com.fasterxml.jackson.databind.ObjectMapper;
import uk.co.jassoft.markets.datamodel.sources.SourceBuilder;
import uk.co.jassoft.markets.datamodel.sources.SourceUrl;
import uk.co.jassoft.markets.datamodel.sources.SourceUrlBuilder;
import uk.co.jassoft.markets.datamodel.story.StoryBuilder;
import uk.co.jassoft.markets.repository.LinkRepository;
import uk.co.jassoft.markets.repository.SourceRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import uk.co.jassoft.network.Network;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.TextMessage;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by jonshaw on 17/03/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class CrawlerListenerTest extends BaseRepositoryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    private StoryRepository storyRepository;

    @Autowired
    private LinkRepository linkRepository;

    @Mock
    private JmsTemplate jmsTemplate;

    @Mock
    private Network network;

    @InjectMocks
    @Autowired
    private CrawlerListener target;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.initMocks(this);

        sourceRepository.deleteAll();
        storyRepository.deleteAll();
        linkRepository.deleteAll();
    }

    @Test
    public void testOnMessage_withDisabledSource_doesNotFindAnyLinks() throws Exception {

        String sourceId = sourceRepository.save(SourceBuilder.aSource()
                .withDisabled(true)
                .build()).getId();

        when(network.read(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(this.getClass().getResourceAsStream("/testWebPage.html"));

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(mapper.writeValueAsString(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setParentSource(sourceId)
                .createStory()));

        target.onMessage(textMessage);
        target.onMessage(textMessage);

        verify(jmsTemplate, never()).convertAndSend(anyString(), any(String.class));
        assertEquals(0, storyRepository.count());
        assertEquals(0, linkRepository.count());
    }

    @Test
    public void testOnMessage_withValidData_findsLinks() throws Exception {

        String sourceId = sourceRepository.save(SourceBuilder.aSource()
                .withDisabled(false)
                .withUrl(SourceUrlBuilder.aSourceUrl().withUrl("http://test.com").withEnabled(true).build())
                .build()).getId();

        Mockito.when(network.read(eq("http://test.com"), eq("GET"), eq(false))).thenReturn(this.getClass().getResourceAsStream("/testWebPage.html"));

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(mapper.writeValueAsString(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setParentSource(sourceId)
                .createStory()));

        target.onMessage(textMessage);

        verify(jmsTemplate, times(144)).convertAndSend(anyString(), any(String.class));

        assertEquals(72, storyRepository.count());
        assertEquals(72, linkRepository.count());
    }
}