import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.*;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.joauth.UrlCodec;
import eventstore.*;
import eventstore.Event;
import eventstore.j.EventDataBuilder;
import eventstore.j.SettingsBuilder;
import eventstore.j.WriteEventsBuilder;
import eventstore.proto.EventStoreMessages;
import eventstore.tcp.ConnectionActor;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import javafx.scene.NodeBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleStream {

    private final Logger slf4jLogger = LoggerFactory.getLogger(SampleStream.class);

    public void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException, IOException {

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100);
        //BlockingQueue<com.twitter.hbc.core.event.Event> queue1 = new LinkedBlockingQueue<com.twitter.hbc.core.event.Event>(100);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        //endpoint.trackTerms(Lists.newArrayList("#WednesdayWisdom"));
        endpoint.locations(Lists.newArrayList(
                new Location(new Location.Coordinate(-122.75, 36.8), new Location.Coordinate(-121.75, 37.8))));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        BasicClient client = new ClientBuilder()
                .name("sampleExampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        // Event Store

        /*for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = queue.take();
            System.out.println(msg);
        }*/

       final Settings settings = new SettingsBuilder().address(new InetSocketAddress("127.0.0.1", 1113))
                .defaultCredentials("admin", "changeit").build();

        final ActorSystem system = ActorSystem.create();
        final ActorRef connection = system.actorOf(ConnectionActor.getProps(settings));
        final ActorRef writeResult = system.actorOf(Props.create(WriteResult.class));

        //Gson gson = new GsonBuilder().setPrettyPrinting().create();

        List<EventData> events =  new ArrayList<EventData>();

       /* while (!client.isDone()) {
            for (int msgRead = 0; msgRead < 10; msgRead++) {
                String msg = queue.poll();
                slf4jLogger.info("Tweet" + msgRead);
                //slf4jLogger.info(msg);

                events.add(new EventDataBuilder("sample-event"+msg).eventId(UUID.randomUUID()).data(msg).build());
                //events.add(new EventDataBuilder("sample-event").data(msg).build());
            }

            final WriteEvents writeEvents = new WriteEventsBuilder("TweetStream12").addEvents(events).expectAnyVersion().build();

            connection.tell(writeEvents, writeResult);
            events.clear();
        }*/

       /* // Redis
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 90);
        Jedis jedis = pool.getResource();

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        for (int msgRead = 0; msgRead < 10; msgRead++) {
            String msg = queue.take();
            JsonObject o = gson.fromJson(msg, JsonElement.class).getAsJsonObject();
            jedis.set(o.get("id").getAsInt()+"",o.toString());
            //System.out.println("Tweet with Id " + o.get("id").getAsInt() + " inserted");
        }*/

        //ElasticSearch
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        JestClientFactory factory = new JestClientFactory();

        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient jestClient = factory.getObject();

        System.out.println("Jest Created");

        CreateIndex createIndex = new CreateIndex.Builder("tweeting").build();
        jestClient.execute(createIndex);

        System.out.println("Index Created");

        for (int msgRead = 0; msgRead < 10; msgRead++) {
            String msg = queue.take();
            JsonObject json = gson.fromJson(msg, JsonElement.class).getAsJsonObject();

            System.out.println(json);

            Index index = new Index.Builder(json).index("tweeting").type("tweet").build();
            jestClient.execute(index);
            System.out.println("Created");
        }

        jestClient.shutdownClient();
        //client.stop();

        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }

    public static class WriteResult extends UntypedActor {

        final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

        public void onReceive(Object message) throws Exception {
            if (message instanceof EventStoreMessages.WriteEventsCompleted) {
                final WriteEventsCompleted completed = (WriteEventsCompleted) message;
                log.info("range: {}, position: {}", completed.numbersRange(), completed.position());
            } else if (message instanceof Status.Failure) {
                final Status.Failure failure = ((Status.Failure) message);
                final EsException exception = (EsException) failure.cause();
                log.error(exception, exception.toString());
            } else
                unhandled(message);

            context().system().terminate();
        }
    }

    public static void main(String[] args) {
        try {
            SampleStream sampleStream = new SampleStream();
            sampleStream.run(args[0], args[1], args[2], args[3]);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        catch (IOException e) {
            System.out.println(e);
        }
    }
}
