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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Drives the search fetch path so the coordinator holds large fetched responses in heap while a slow
 * client reads them. Each reader opens a raw socket, sends a full-source search that pulls ~200 MB of
 * _source, then drains the response body a few KB at a time, so the coordinator blocks on the write and
 * keeps the fetched hits referenced. A few readers start staggered, so each fetch transit stays under
 * the request breaker while the held sources accumulate on node 0 to more than its heap. On a build
 * that does not charge the coordinator hold, those held sources exhaust node 0's heap and it dies.
 */
public class LargeSourceSearchResponsesIT extends ESRestTestCase {

    private static final String INDEX = "large-source-search";
    // 20 small shards so the data node's per-shard fetch stays under its own request breaker and
    // the hits reach the coordinator, where they pile up.
    private static final int SHARDS = 20;
    // 900 docs of ~1 MB source each; plenty in the index to serve each response.
    private static final int DOC_COUNT = 900;
    private static final int DOC_BYTES = 1024 * 1024;
    private static final int DOCS_PER_BULK = 10;
    // Pull ~200 hits per response (~200 MB) so a single response's transit stays just under the 307 MB
    // request breaker, but the held _source is large.
    private static final int SEARCH_SIZE = 200;
    // Only a few slow readers; 4 held ~200 MB responses is ~800 MB of uncharged held _source on node 0.
    private static final int SLOW_READERS = 4;
    // Stagger the reader starts so their fetch transits do not overlap and sum past the breaker; each
    // fetch completes and its _source is held while the next starts.
    private static final int STAGGER_MILLIS = 200;
    // Read the body in small sips with a pause between them, so node 0 holds each whole response in heap.
    private static final int READ_CHUNK_BYTES = 16 * 1024;
    private static final int READ_DELAY_MILLIS = 100;
    private static final int HOLD_READS = 200;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .node(0, n -> n.setting("node.roles", "[]"))
        .node(1, n -> n.jvmArg("-Xms2g").jvmArg("-Xmx2g"))
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        // Drive all REST traffic at the coordinator-only node 0, which holds the assembled hits.
        return cluster.getHttpAddress(0);
    }

    public void testHeldSourceResponsesDoNotKillTheNode() throws Exception {
        createAndLoadIndex();

        List<Integer> statuses = Collections.synchronizedList(new ArrayList<>());
        List<String> breakerLabels = Collections.synchronizedList(new ArrayList<>());
        List<String> breakerBodies = Collections.synchronizedList(new ArrayList<>());

        // Staggered starts so fetch transits do not overlap; the held sources accumulate across readers.
        ExecutorService pool = Executors.newFixedThreadPool(SLOW_READERS);
        List<Future<Void>> futures = new ArrayList<>(SLOW_READERS);
        try {
            for (int i = 0; i < SLOW_READERS; i++) {
                final int index = i;
                futures.add(pool.submit(() -> {
                    Thread.sleep((long) index * STAGGER_MILLIS);
                    slowSearch(statuses, breakerLabels, breakerBodies);
                    return null;
                }));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdown();
        }

        // Print status counts, breaker labels, and one 429 body so the run table can name the reservation.
        int ok = 0, tooMany = 0, connErr = 0, other = 0;
        for (int code : statuses) {
            if (code >= 200 && code < 300) {
                ok++;
            } else if (code == 429) {
                tooMany++;
            } else if (code == 0) {
                connErr++;
            } else {
                other++;
            }
        }
        logger.info("search status counts: 2xx={} 429={} connErr(0)={} other={} (total {})", ok, tooMany, connErr, other, statuses.size());
        logger.info("breaker labels seen: {}", breakerLabels);
        if (breakerBodies.isEmpty() == false) {
            logger.info("one 429 body: {}", breakerBodies.get(0));
        }

        // Breaker gap: every response must be 2xx or 429. A dropped connection (0) means node 0 died.
        for (int code : statuses) {
            boolean allowed = (code >= 200 && code < 300) || code == 429;
            assertTrue("expected 2xx or 429 but got " + code, allowed);
        }

        // At least one refusal proves the load reached the hold and the breaker charged it; without this
        // the test would pass even if the load were too weak to exercise the path.
        assertThat(
            "the coordinator fetch breaker should refuse at least one request, else the load did not reach the hold",
            tooMany,
            greaterThan(0)
        );

        // The coordinator must still answer after the load.
        assertOK(client().performRequest(new Request("GET", "/_cluster/health")));
    }

    // Opens a raw socket to node 0, sends a full-source search, then reads the response slowly so the
    // coordinator holds it in heap. Records the status; status 0 marks a dropped connection (node death).
    private void slowSearch(List<Integer> statuses, List<String> breakerLabels, List<String> breakerBodies) throws InterruptedException {
        String hostAndPort = cluster.getHttpAddress(0);
        int colon = hostAndPort.lastIndexOf(':');
        String host = hostAndPort.substring(0, colon);
        int port = Integer.parseInt(hostAndPort.substring(colon + 1));

        byte[] body = ("{\"size\":" + SEARCH_SIZE + ",\"query\":{\"match_all\":{}}}").getBytes(StandardCharsets.UTF_8);
        String requestHead = "POST /"
            + INDEX
            + "/_search HTTP/1.1\r\n"
            + "Host: "
            + host
            + ":"
            + port
            + "\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: "
            + body.length
            + "\r\n"
            + "Connection: close\r\n"
            + "\r\n";

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(120000);
            OutputStream out = socket.getOutputStream();
            out.write(requestHead.getBytes(StandardCharsets.US_ASCII));
            out.write(body);
            out.flush();

            InputStream in = socket.getInputStream();
            // Read status line and headers (small) up to the blank line that ends them.
            ByteArrayOutputStream head = new ByteArrayOutputStream();
            boolean headersComplete = false;
            int b;
            while ((b = in.read()) != -1) {
                head.write(b);
                byte[] seen = head.toByteArray();
                int n = seen.length;
                if (n >= 4 && seen[n - 4] == '\r' && seen[n - 3] == '\n' && seen[n - 2] == '\r' && seen[n - 1] == '\n') {
                    headersComplete = true;
                    break;
                }
            }
            if (headersComplete == false) {
                // Connection closed before a full response arrived: under this load node 0 died.
                statuses.add(0);
                return;
            }
            int status = parseStatus(head.toString(StandardCharsets.US_ASCII.name()));
            statuses.add(status);

            if (status >= 200 && status < 300) {
                // Hold the completed response: sip the large body slowly so the server blocks on the write.
                byte[] buffer = new byte[READ_CHUNK_BYTES];
                try {
                    for (int i = 0; i < HOLD_READS; i++) {
                        Thread.sleep(READ_DELAY_MILLIS);
                        if (in.read(buffer) == -1) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    // Connection reset mid-hold: node 0 died while holding the response.
                }
            } else {
                // Error body is small; read it fully and pull out the breaker reservation label.
                ByteArrayOutputStream bodyOut = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                try {
                    int r;
                    while ((r = in.read(buffer)) != -1) {
                        bodyOut.write(buffer, 0, r);
                    }
                } catch (IOException e) {
                    // Partial error body is still enough to read the breaker label.
                }
                String errorBody = bodyOut.toString(StandardCharsets.UTF_8.name());
                if (status == 429) {
                    breakerBodies.add(errorBody);
                    breakerLabels.add(extractBreakerLabel(errorBody));
                }
            }
        } catch (IOException e) {
            // Connect refused or reset before a status: node 0 is unreachable, which under this load means it died.
            statuses.add(0);
        }
    }

    private static int parseStatus(String headers) {
        int eol = headers.indexOf("\r\n");
        String statusLine = eol >= 0 ? headers.substring(0, eol) : headers;
        String[] parts = statusLine.split(" ");
        return Integer.parseInt(parts[1]);
    }

    private static String extractBreakerLabel(String errorBody) {
        if (errorBody.contains("fetch[coordinator]")) {
            return "fetch[coordinator]";
        }
        if (errorBody.contains("fetch[chunk]")) {
            return "fetch[chunk]";
        }
        int marker = errorBody.indexOf("data for [");
        if (marker >= 0) {
            int from = marker + "data for [".length();
            int end = errorBody.indexOf(']', from);
            if (end > from) {
                return errorBody.substring(from, end);
            }
        }
        return "unknown";
    }

    private void createAndLoadIndex() throws Exception {
        Request create = new Request("PUT", "/" + INDEX);
        // Disable mappings so the ~1 MB field is stored in _source but never indexed or analyzed.
        create.setJsonEntity(
            "{\"settings\":{\"number_of_shards\":" + SHARDS + ",\"number_of_replicas\":0},\"mappings\":{\"enabled\":false}}"
        );
        client().performRequest(create);

        String filler = randomFiller();
        StringBuilder bulk = new StringBuilder();
        int inBatch = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            bulk.append("{\"index\":{\"_index\":\"").append(INDEX).append("\"}}\n");
            bulk.append("{\"data\":\"").append(filler).append("\"}\n");
            inBatch++;
            if (inBatch == DOCS_PER_BULK || i == DOC_COUNT - 1) {
                Request bulkRequest = new Request("POST", "/_bulk");
                bulkRequest.setJsonEntity(bulk.toString());
                client().performRequest(bulkRequest);
                bulk.setLength(0);
                inBatch = 0;
            }
        }
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    }

    // One ~1 MB synthetic string of ASCII letters, built with a fixed seed so it is deterministic
    // and resists compression enough to occupy real heap when held per hit on the coordinator.
    private static String randomFiller() {
        Random random = new Random(3278);
        char[] chars = new char[DOC_BYTES];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ('a' + random.nextInt(26));
        }
        return new String(chars);
    }
}
