package org.elasticsearch.graphql.server;

import org.reactivestreams.Publisher;

public interface DemoServerSocket {

    Publisher<String> getIncomingMessages();

    void send(String message);
}
