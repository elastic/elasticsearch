/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.search.suggest.document.CompletionTokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests the analyzer built by {@link IndexShard#buildIndexAnalyzer} when token count metrics are enabled. The index
 * analyzers count how often they create {@link Analyzer.TokenStreamComponents}, which shows whether analysis chains
 * are reused per analyzer (expected) or per field.
 */
public class TokenCountingIndexAnalyzerTests extends MapperServiceTestCase {

    private final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
    private final ComponentsCountingAnalyzer defaultAnalyzer = new ComponentsCountingAnalyzer();
    private final ComponentsCountingAnalyzer otherAnalyzer = new ComponentsCountingAnalyzer();

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, defaultAnalyzer),
                "other",
                new NamedAnalyzer("other", AnalyzerScope.INDEX, otherAnalyzer)
            )
        );
    }

    public void testReusesComponentsAcrossFieldsSharingAnAnalyzer() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("a").field("type", "text").endObject();
            b.startObject("b").field("type", "text").endObject();
            b.startObject("c").field("type", "text").field("position_increment_gap", 7).endObject();
            b.startObject("d").field("type", "text").field("analyzer", "other").endObject();
        }));
        Analyzer analyzer = buildIndexAnalyzer(mapperService);

        TokenStream a = consume(analyzer, "a", "one two");
        assertThat(consume(analyzer, "b", "one two"), sameInstance(a));
        assertThat(consume(analyzer, "c", "one two"), sameInstance(a));
        TokenStream d = consume(analyzer, "d", "one two");
        assertThat(d, not(sameInstance(a)));
        assertThat(consume(analyzer, "d", "one two"), sameInstance(d));

        assertThat(defaultAnalyzer.created.get(), equalTo(1));
        assertThat(otherAnalyzer.created.get(), equalTo(1));
    }

    public void testUsesGapsOfEachField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("a").field("type", "text").endObject();
            b.startObject("c").field("type", "text").field("position_increment_gap", 7).endObject();
        }));
        Analyzer analyzer = buildIndexAnalyzer(mapperService);

        assertThat(analyzer.getPositionIncrementGap("c"), equalTo(7));
        for (String field : List.of("a", "c")) {
            NamedAnalyzer fieldAnalyzer = mapperService.indexAnalyzer(field, f -> { throw new AssertionError(); });
            assertThat(analyzer.getPositionIncrementGap(field), equalTo(fieldAnalyzer.getPositionIncrementGap(field)));
            assertThat(analyzer.getOffsetGap(field), equalTo(fieldAnalyzer.getOffsetGap(field)));
        }
    }

    public void testRecordsTokenCountPerFieldValue() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("a").field("type", "text").endObject();
            b.startObject("b").field("type", "text").endObject();
            b.startObject("d").field("type", "text").field("analyzer", "other").endObject();
        }));
        Analyzer analyzer = buildIndexAnalyzer(mapperService);

        consume(analyzer, "a", "one two three");
        consume(analyzer, "b", "one");
        consume(analyzer, "a", "one two");
        consume(analyzer, "d", "one two three four");

        assertThat(recordedTokenCounts(), containsInAnyOrder(3L, 1L, 2L, 4L));
    }

    /**
     * A mapping update discards the wrappers so that analyzers that are no longer mapped are not retained. The
     * chains are rebuilt once per analyzer and thread, and shared across fields again afterwards.
     */
    public void testMappingUpdateRebuildsComponents() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("a").field("type", "text").endObject()));
        Analyzer analyzer = buildIndexAnalyzer(mapperService);

        TokenStream before = consume(analyzer, "a", "one two");
        merge(mapperService, mapping(b -> b.startObject("b").field("type", "text").endObject()));
        TokenStream after = consume(analyzer, "a", "one two");

        assertThat(after, not(sameInstance(before)));
        assertThat(consume(analyzer, "b", "one two"), sameInstance(after));
        assertThat(defaultAnalyzer.created.get(), equalTo(2));
    }

    public void testDoesNotCountCompletionFields() throws IOException {
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject("suggest").field("type", "completion").field("analyzer", "default").endObject())
        );
        Analyzer analyzer = buildIndexAnalyzer(mapperService);

        try (TokenStream ts = analyzer.tokenStream("suggest", "one two")) {
            assertThat(ts, instanceOf(CompletionTokenStream.class));
        }
    }

    public void testConcurrentIndexing() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            for (int i = 0; i < 10; i++) {
                b.startObject("field" + i).field("type", "text").endObject();
            }
        }));
        Analyzer analyzer = buildIndexAnalyzer(mapperService);
        int threads = randomIntBetween(2, 8);
        int iterations = randomIntBetween(10, 50);

        runInParallel(threads, t -> {
            for (int i = 0; i < iterations; i++) {
                for (int f = 0; f < 10; f++) {
                    try {
                        consume(analyzer, "field" + f, "one two three");
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        });

        List<Long> counts = recordedTokenCounts();
        assertThat(counts.size(), equalTo(threads * iterations * 10));
        assertTrue(counts.stream().allMatch(c -> c == 3L));
        assertThat(defaultAnalyzer.created.get(), greaterThanOrEqualTo(1));
        assertThat(defaultAnalyzer.created.get(), lessThanOrEqualTo(threads));
    }

    public void testConcurrentIndexingDuringMappingUpdates() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            for (int i = 0; i < 5; i++) {
                b.startObject("field" + i).field("type", "text").endObject();
            }
        }));
        Analyzer analyzer = buildIndexAnalyzer(mapperService);
        int threads = randomIntBetween(2, 8);
        int iterations = randomIntBetween(20, 100);
        int mappingUpdates = randomIntBetween(5, 20);

        runInParallel(threads + 1, t -> {
            try {
                if (t == threads) {
                    for (int u = 0; u < mappingUpdates; u++) {
                        String field = "new_field" + u;
                        merge(mapperService, mapping(b -> b.startObject(field).field("type", "text").endObject()));
                    }
                } else {
                    for (int i = 0; i < iterations; i++) {
                        for (int f = 0; f < 5; f++) {
                            consume(analyzer, "field" + f, "one two three");
                        }
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });

        List<Long> counts = recordedTokenCounts();
        assertThat(counts.size(), equalTo(threads * iterations * 5));
        assertTrue(counts.stream().allMatch(c -> c == 3L));
        for (int u = 0; u < mappingUpdates; u++) {
            consume(analyzer, "new_field" + u, "one two");
        }
    }

    private Analyzer buildIndexAnalyzer(MapperService mapperService) {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        TokenCountingMetrics metrics = new TokenCountingMetrics(telemetryPlugin.getTelemetryProvider(settings).getMeterRegistry());
        Analyzer analyzer = IndexShard.buildIndexAnalyzer(mapperService, metrics);
        assertThat(analyzer, instanceOf(TokenCountingIndexAnalyzer.class));
        return analyzer;
    }

    private List<Long> recordedTokenCounts() {
        return telemetryPlugin.getLongHistogramMeasurement(TokenCountingMetrics.FIELD_TOKEN_COUNT)
            .stream()
            .map(Measurement::getLong)
            .toList();
    }

    private static TokenStream consume(Analyzer analyzer, String field, String text) throws IOException {
        try (TokenStream ts = analyzer.tokenStream(field, text)) {
            ts.reset();
            while (ts.incrementToken()) {
                // consume
            }
            ts.end();
            return ts;
        }
    }

    /**
     * Counts how often analysis chains are built. Uses the default global reuse strategy, like most analyzers.
     */
    private static final class ComponentsCountingAnalyzer extends Analyzer {
        private final AtomicInteger created = new AtomicInteger();

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            created.incrementAndGet();
            return new TokenStreamComponents(new StandardTokenizer());
        }
    }
}
