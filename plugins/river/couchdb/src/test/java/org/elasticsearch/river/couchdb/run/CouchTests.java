package org.elasticsearch.river.couchdb.run;

import org.elasticsearch.river.couchdb.http.Http;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.*;

@Test
public class CouchTests {

    Http http = new Http();

    @Test
    public void canStartACouchDBAndDetermineAPort() throws Exception {
        Couch couch = new Couch(http);
        couch.start();

        URI uri = couch.uri();

        try {
            assertCanConnectTo(uri);
        }
        finally {
            couch.stop();
        }
    }

    private void assertCanConnectTo(URI uri) throws IOException {
        Socket socket = new Socket();
        SocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
        try {
            socket.connect(address, 1000);
        }
        catch (ConnectException e) {
            throw new IOException("Cannot make socket connection to " + uri, e);
        }
        socket.close();
    }
}
