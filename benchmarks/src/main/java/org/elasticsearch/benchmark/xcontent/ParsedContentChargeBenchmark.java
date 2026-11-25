/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures what charging the request breaker for parsed REST content costs, by parsing request bodies with and without the
 * charging parser that {@code RestRequest} hands out. The tracked mode follows a request through: a reservation, a tracked
 * parser, the parse, closing the parser and releasing the reservation when the response is sent.
 */
@Fork(2)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ParsedContentChargeBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "match_tiny", "bool_typical", "dashboard", "mapping_map", "terms_10k", "terms_10k_mixed", "knn_1024" })
    public String body;

    @Param({ "plain", "tracked" })
    public String mode;

    private byte[] bytes;
    private XContentParserConfiguration config;
    private CircuitBreaker breaker;
    private boolean asMap;

    @Setup(Level.Trial)
    public void setup() {
        config = XContentParserConfiguration.EMPTY.withRegistry(
            new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents())
        );
        breaker = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
        asMap = body.equals("mapping_map");
        bytes = body(body).getBytes(StandardCharsets.UTF_8);
    }

    @Benchmark
    public Object parse() throws IOException {
        if (mode.equals("plain")) {
            try (XContentParser parser = XContentType.JSON.xContent().createParser(config, bytes)) {
                return parse(parser);
            }
        }
        ParsedContentReservation reservation = new ParsedContentReservation(breaker, true);
        try {
            try (XContentParser parser = reservation.track(XContentType.JSON.xContent().createParser(config, bytes))) {
                return parse(parser);
            }
        } finally {
            reservation.close();
        }
    }

    private Object parse(XContentParser parser) throws IOException {
        if (asMap) {
            return parser.map();
        }
        return new SearchSourceBuilder().parseXContent(parser, true, f -> true);
    }

    static String body(String name) {
        Random random = new Random(42);
        return switch (name) {
            case "match_tiny" -> """
                {"query":{"match":{"message":"connection refused"}}}""";
            case "bool_typical" -> """
                {"size":20,"track_total_hits":true,"_source":["@timestamp","message","host.name","service.name"],
                 "sort":[{"@timestamp":{"order":"desc"}}],
                 "query":{"bool":{
                   "must":[{"match":{"message":{"query":"connection refused upstream","operator":"and"}}}],
                   "filter":[{"term":{"service.name":"checkout"}},{"terms":{"host.name":["web-1","web-2","web-3"]}},
                             {"range":{"@timestamp":{"gte":"now-15m","lte":"now"}}}],
                   "must_not":[{"term":{"log.level":"debug"}}]}},
                 "aggs":{"hosts":{"terms":{"field":"host.name","size":10}}}}""";
            case "dashboard" -> dashboard();
            case "mapping_map" -> mapping(random);
            case "terms_10k" -> terms(random, 10_000, false);
            case "terms_10k_mixed" -> terms(random, 10_000, true);
            case "knn_1024" -> knn(random, 1024);
            default -> throw new IllegalArgumentException("unknown body [" + name + "]");
        };
    }

    private static String dashboard() {
        StringBuilder b = new StringBuilder(
            """
                {"size":0,"query":{"bool":{"filter":[{"range":{"@timestamp":{"gte":"now-24h","lte":"now","format":"strict_date_optional_time"}}},
                {"bool":{"should":[{"match_phrase":{"service.name":"checkout"}},{"match_phrase":{"service.name":"cart"}}],
                "minimum_should_match":1}}]}},"aggs":{"""
        );
        for (int i = 0; i < 8; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(String.format(Locale.ROOT, """
                "panel_%d":{"date_histogram":{"field":"@timestamp","fixed_interval":"30m","min_doc_count":0},
                "aggs":{"by_host":{"terms":{"field":"host.name","size":5,"order":{"latency":"desc"}},
                "aggs":{"latency":{"avg":{"field":"event.duration"}},"p99":{"percentiles":{"field":"event.duration","percents":[50,95,99]}},
                "errors":{"filter":{"term":{"log.level":"error"}}}}}}}""", i));
        }
        return b.append("}}").toString();
    }

    private static String mapping(Random random) {
        StringBuilder b = new StringBuilder("{\"properties\":{");
        for (int i = 0; i < 200; i++) {
            if (i > 0) {
                b.append(',');
            }
            String type = switch (random.nextInt(4)) {
                case 0 -> "{\"type\":\"keyword\",\"ignore_above\":1024}";
                case 1 -> "{\"type\":\"long\"}";
                case 2 -> "{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"}";
                default -> "{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}";
            };
            b.append("\"field_").append(i).append("\":").append(type);
        }
        return b.append("}}").toString();
    }

    private static String terms(Random random, int count, boolean mixed) {
        StringBuilder b = new StringBuilder("{\"query\":{\"terms\":{\"id\":[");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('"');
            for (int c = 0; c < 16; c++) {
                b.append(mixed && c % 4 == 0 ? (char) ('é' + random.nextInt(20)) : (char) ('a' + random.nextInt(26)));
            }
            b.append('"');
        }
        return b.append("]}}}").toString();
    }

    private static String knn(Random random, int dims) {
        StringBuilder b = new StringBuilder("{\"knn\":{\"field\":\"embedding\",\"k\":10,\"num_candidates\":100,\"query_vector\":[");
        for (int i = 0; i < dims; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(random.nextFloat() * 2 - 1);
        }
        return b.append("]}}").toString();
    }
}
