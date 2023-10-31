/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.test.ESTestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class RecordingApmServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(RecordingApmServer.class);

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    final ArrayBlockingQueue<String> received = new ArrayBlockingQueue<>(1000);

    private static HttpServer server;
    private volatile Consumer<String> consumer;
    private Thread thread;
    private volatile boolean consumerRunning = true;

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();

        this.thread = new Thread(consumerTask());
        thread.start();
    }

    private Runnable consumerTask() {
        return () -> {
            while (consumerRunning) {
                if (consumer != null) {
                    try {
                        String msg = received.poll(1L, TimeUnit.SECONDS);
                        if (msg != null && msg.isEmpty() == false) {
                            System.out.println("received" + msg);
                            consumer.accept(msg);
                        }

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    @Override
    protected void after() {
        server.stop(1);
        consumerRunning = false;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                try (InputStream requestBody = exchange.getRequestBody()) {
                    if (requestBody != null) {
                        var read = readJsonMessages(requestBody);
                        read.forEach(s -> logger.debug(s));
                        received.addAll(read);
                    }
                }

            } catch (RuntimeException e) {
                logger.warn("failed to parse request", e);
            }
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private List<String> readJsonMessages(InputStream input) throws IOException {
        // parse NDJSON
        return Arrays.stream(new String(input.readAllBytes(), StandardCharsets.UTF_8).split(System.lineSeparator())).toList();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public List<String> getMessages() {
        List<String> list = new ArrayList<>(received.size());
        received.drainTo(list);
        return list;
    }

    public CountDownLatch addMessageAssertions(Set<Predicate<APMMessage>> assertions) {
        CountDownLatch success = new CountDownLatch(1);
        this.consumer = (String message) -> {
            var apmMessage = new APMMessage(message);
            assertions.removeIf(c -> c.test(apmMessage));

            if (assertions.isEmpty()) {
                success.countDown();
            }
        };
        return success;
    }

    public record Bucket(double value, int count) {}

    public static class APMMessage {
        Map<String, List<Bucket>> histograms = new HashMap<>();
        Map<String, Double> doubles = new HashMap<>();
        Map<String, Long> longs = new HashMap<>();

        @SuppressWarnings("unchecked")
        APMMessage(String message) {
            Map<String, Object> parsed;
            try (XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, message)) {
                parsed = parser.map();
            } catch (IOException e) {
                fail(e);
                parsed = Collections.emptyMap();
            }
            if ("elasticsearch".equals(getTags(parsed).get("otel_instrumentation_scope_name")) == false) {
                return;
            }
            getSamples(parsed).forEach((name, value) -> {
                if ("histogram".equals(value.get("type"))) {
                    List<Double> values = (List<Double>) value.getOrDefault("values", Collections.emptyList());
                    List<Integer> counts = (List<Integer>) value.getOrDefault("counts", Collections.emptyList());
                    assertThat(values.size(), equalTo(counts.size()));
                    List<Bucket> buckets = new ArrayList<>(values.size());
                    for (int i = 0; i < values.size(); i++) {
                        buckets.add(new Bucket(values.get(i), counts.get(i)));
                    }
                    histograms.put(name, buckets);
                    return;
                }
                if (value.get("value") instanceof Number number) {
                    if (number instanceof Integer intn) {
                        longs.put(name, Long.valueOf(intn));
                    } else if (number instanceof Long longn) {
                        longs.put(name, longn);
                    } else if (number instanceof Float floatn) {
                        doubles.put(name, Double.valueOf(floatn));
                    } else if (number instanceof Double doublen) {
                        doubles.put(name, doublen);
                    } else {
                        fail("unknown number [" + number.getClass().getName() + "] [" + number + "]");
                    }
                }
            });
        }

        @SuppressWarnings("unchecked")
        private static Map<String, String> getTags(Map<String, Object> parsed) {
            return (Map<String, String>) (getMetricset(parsed).getOrDefault("tags", Collections.emptyMap()));
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Map<String, Object>> getSamples(Map<String, Object> parsed) {
            return (Map<String, Map<String, Object>>) getMetricset(parsed).getOrDefault("samples", Collections.emptyMap());
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Map<String, ?>> getMetricset(Map<String, Object> parsed) {
            if (parsed.get("metricset") instanceof Map<?, ?> map) {
                return (Map<String, Map<String, ?>>) map;
            }
            return Collections.emptyMap();
        }

        public List<Bucket> getHistogram(String name) {
            return histograms.getOrDefault(name, Collections.emptyList());
        }

        public Long getLong(String name) {
            return longs.get(name);
        }

        public Double getDouble(String name) {
            return doubles.get(name);
        }
    }
}
