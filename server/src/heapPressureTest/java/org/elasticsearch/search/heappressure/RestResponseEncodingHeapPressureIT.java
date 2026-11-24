/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.SearchHeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A search response is encoded a chunk at a time, but a hit is never split, so a hit with a large {@code _source} becomes a chunk of about
 * its size, which stays on the heap until its write completes. Each search here returns one {@link #DOC_MB} MB document to a client that
 * never reads it. Without charging the chunks, the node runs out of memory before the request breaker rejects a search.
 */
public class RestResponseEncodingHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "rest-encoding";
    /** Well over the 1 MB below which RestController does not charge a chunk. */
    private static final int DOC_MB = 8;
    private static final int DOCS = 4;
    /** Enough unread responses for their hits alone to exceed the 153 MB request breaker limit of the 256 MB node. */
    private static final int MAX_UNREAD = 30;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m")
        // An 8 MB document counts as about 34 MB of indexing pressure, over the 27 MB primary limit of a 256 MB heap. Only the setup
        // indexes, one document at a time.
        .setting("indexing_pressure.memory.limit", "50%")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexLargeDocuments() throws IOException {
        if (indexExists(INDEX)) {
            return;
        }
        Request create = new Request("PUT", "/" + INDEX);
        // Two shards, since a search of a single shard fetches without charging the hits on the coordinating node.
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": 2, "number_of_replicas": 0},
              "mappings": {"properties": {"tag": {"type": "keyword"}, "big": %s}}
            }
            """, SOURCE_ONLY_KEYWORD));
        client().performRequest(create);
        String big = "x".repeat(DOC_MB * 1024 * 1024);
        for (int i = 0; i < DOCS; i++) {
            Request index = new Request("PUT", "/" + INDEX + "/_doc/" + i);
            index.setJsonEntity(Strings.format("""
                {"tag":"tag-%d","big":"%s"}""", i, big));
            client().performRequest(index);
            // See SearchHeapPressureTestCase#indexDocuments: the translog keeps every operation until it is rolled.
            client().performRequest(new Request("POST", "/" + INDEX + "/_flush"));
        }
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        ensureGreen(INDEX);
    }

    public void testUnreadSearchResponsesTripBreaker() throws Exception {
        long breakerAtStart = requestBreakerUsedBytes();
        long trippedAtStart = trippedCount();
        List<Socket> unread = new ArrayList<>();
        try {
            for (int i = 0; i < MAX_UNREAD; i++) {
                long breakerBefore = requestBreakerUsedBytes();
                int status = sendUnreadSearch(openUnreadSocket(unread), i % DOCS);
                assertNotEquals("search [" + i + "] got no response, most likely as the node ran out of memory", -1, status);
                if (status == 429) {
                    logger.info("search [{}] rejected with 429 at request breaker [{}] bytes", i, breakerBefore);
                    break;
                }
                assertEquals("unexpected status for search [" + i + "]", 200, status);
                // The status line goes out before the body, so wait for at least the hit's charge before sending the next search.
                long expected = breakerBefore + DOC_MB * 1024L * 1024L;
                assertBusy(() -> {
                    assertTrue("a node exited, most likely on an OutOfMemoryError", nodesAreAlive());
                    long used = requestBreakerUsedBytes();
                    assertTrue("request breaker [" + used + "] below expected [" + expected + "]", used >= expected);
                }, 30, TimeUnit.SECONDS);
                logger.info("unread search responses [{}] request breaker [{}] bytes", i + 1, requestBreakerUsedBytes());
            }
            assertTrue("a node exited, most likely on an OutOfMemoryError", nodesAreAlive());
        } finally {
            IOUtils.closeWhileHandlingException(unread);
        }
        assertNodeAlive();
        assertTrue("expected the request breaker to reject a search", trippedCount() > trippedAtStart);
        assertBreakerReturnsTo(breakerAtStart);
    }

    /** Closing the connections fails the pending writes, which must release what their responses charged, the hits and their chunks. */
    private void assertBreakerReturnsTo(long expected) throws Exception {
        assertBusy(() -> assertEquals(expected, requestBreakerUsedBytes()), 30, TimeUnit.SECONDS);
    }

    /**
     * A connection with a small receive buffer, so the node can send little of a response before it has to wait for the client. It is
     * added to {@code unread} before it connects, so that it is closed even if it fails to.
     */
    @SuppressForbidden(reason = "a client that never reads its response, which the REST client always reads")
    private Socket openUnreadSocket(List<Socket> unread) throws IOException {
        // Such as 127.0.0.1:9200 or [::1]:9200.
        String address = cluster.getHttpAddresses().split(",")[0];
        int colon = address.lastIndexOf(':');
        String host = address.substring(0, colon).replace("[", "").replace("]", "");
        Socket socket = new Socket();
        unread.add(socket);
        socket.setReceiveBufferSize(4096);
        socket.setSoTimeout(30_000);
        socket.connect(new InetSocketAddress(InetAddresses.forString(host), Integer.parseInt(address.substring(colon + 1))));
        return socket;
    }

    /**
     * Sends a search for one document and reads only the status line of the response, leaving the rest unread. Returns -1 if the
     * connection closed without a response.
     */
    private static int sendUnreadSearch(Socket socket, int doc) throws IOException {
        byte[] body = Strings.format("""
            {"size": 1, "query": {"term": {"tag": "tag-%d"}}}""", doc).getBytes(StandardCharsets.UTF_8);
        OutputStream out = socket.getOutputStream();
        out.write(Strings.format("""
            POST /%s/_search?request_cache=false HTTP/1.1\r
            Host: localhost\r
            Content-Type: application/json\r
            Content-Length: %d\r
            \r
            """, INDEX, body.length).getBytes(StandardCharsets.UTF_8));
        out.write(body);
        out.flush();
        String statusLine = readLine(socket.getInputStream());
        if (statusLine.isEmpty()) {
            return -1;
        }
        // Such as "HTTP/1.1 200 OK".
        return Integer.parseInt(statusLine.split(" ")[1]);
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1 && b != '\n') {
            if (b != '\r') {
                line.write(b);
            }
        }
        return line.toString(StandardCharsets.UTF_8);
    }
}
