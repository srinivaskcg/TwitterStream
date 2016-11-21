import akka.actor.ActorSystem;
import eventstore.IndexedEvent;
import eventstore.SubscriptionObserver;
import eventstore.j.EsConnection;
import eventstore.j.EsConnectionFactory;

import java.io.Closeable;

public class eventReader {
	
	public static void main(String args[]){
		final ActorSystem system = ActorSystem.create();
        final EsConnection connection = EsConnectionFactory.create(system);
        connection.subscribeToAll(new SubscriptionObserver<IndexedEvent>() {
            public void onLiveProcessingStart(Closeable subscription) {
                system.log().info("live processing started");
            }

            public void onEvent(IndexedEvent event, Closeable subscription) {
                system.log().info(event.toString());
            }

            public void onError(Throwable e) {
                system.log().error(e.toString());
            }

            public void onClose() {
                system.log().error("subscription closed");
            }
        }, false, null);
	}
}
