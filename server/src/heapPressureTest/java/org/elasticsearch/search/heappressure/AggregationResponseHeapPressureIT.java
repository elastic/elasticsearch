/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
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
 * A response holds the aggregations the coordinating node reduced until it is sent. Each search here returns a {@link #TAGS} bucket
 * {@code terms} aggregation to a client that never reads it, so the aggregations stay on the heap until the connection closes. Their charge
 * must stay on the request breaker meanwhile, and be about their heap, or the node runs out of memory before a search is rejected.
 */
public class AggregationResponseHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "aggregation-responses";
    /** Under the default {@code search.max_buckets} of 65,536. */
    private static final int TAGS = 60_000;
    private static final int TAG_LENGTH = 32;
    private static final int MAX_UNREAD = 60;

    @ClassRule
    // Each unread response holds about 13 MB, so this rejects the 9th, well before the node would run out of memory on the 15th.
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m", "100mb").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexTags() throws IOException {
        indexDocuments(
            INDEX,
            2,
            """
                {"tag": {"type": "keyword"}, "n": {"type": "long"}}""",
            TAGS,
            i -> Strings.format("{\"tag\":\"%0" + TAG_LENGTH + "d\",\"n\":%d}", i, i)
        );
    }

    public void testUnreadAggregationResponsesTripBreaker() throws Exception {
        long breakerAtStart = requestBreakerUsedBytes();
        long trippedAtStart = trippedCount();
        List<Socket> unread = new ArrayList<>();
        try {
            for (int i = 0; i < MAX_UNREAD; i++) {
                long breakerBefore = requestBreakerUsedBytes();
                int status = sendUnreadSearch(openUnreadSocket(unread));
                assertNotEquals("search [" + i + "] got no response, most likely as the node ran out of memory", -1, status);
                if (status == 429) {
                    logger.info("search [{}] rejected with 429 at request breaker [{}] bytes", i, breakerBefore);
                    break;
                }
                assertEquals("unexpected status for search [" + i + "]", 200, status);
                assertTrue("a node exited, most likely on an OutOfMemoryError", nodesAreAlive());
                logger.info("unread aggregation responses [{}] request breaker [{}] bytes", i + 1, requestBreakerUsedBytes());
            }
            assertTrue("a node exited, most likely on an OutOfMemoryError", nodesAreAlive());
        } finally {
            IOUtils.closeWhileHandlingException(unread);
        }
        assertNodeAlive();
        assertTrue("expected the request breaker to reject a search", trippedCount() > trippedAtStart);
        assertBusy(() -> assertEquals(breakerAtStart, requestBreakerUsedBytes()), 30, TimeUnit.SECONDS);
    }

    @SuppressForbidden(reason = "a client that never reads its response, which the REST client always reads")
    private Socket openUnreadSocket(List<Socket> unread) throws IOException {
        // Such as 127.0.0.1:9200 or [::1]:9200.
        String address = cluster.getHttpAddresses().split(",")[0];
        int colon = address.lastIndexOf(':');
        String host = address.substring(0, colon).replace("[", "").replace("]", "");
        Socket socket = new Socket();
        unread.add(socket);
        socket.setReceiveBufferSize(4096);
        socket.setSoTimeout(60_000);
        socket.connect(new InetSocketAddress(InetAddresses.forString(host), Integer.parseInt(address.substring(colon + 1))));
        return socket;
    }

    private static int sendUnreadSearch(Socket socket) throws IOException {
        byte[] body = Strings.format("""
            {
              "size": 0,
              "aggs": {"tags": {"terms": {"field": "tag", "size": %d}, "aggs": {"max_n": {"max": {"field": "n"}}}}}
            }""", TAGS).getBytes(StandardCharsets.UTF_8);
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
