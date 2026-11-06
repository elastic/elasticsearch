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
import java.util.ArrayList;
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
 * are reused per analyzer across fields and shards (expected) or per field.
 */
public class TokenCountingIndexAnalyzerTests extends MapperServiceTestCase {

    private final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
    private final ComponentsCountingAnalyzer defaultAnalyzer = new ComponentsCountingAnalyzer();
    private final ComponentsCountingAnalyzer otherAnalyzer = new ComponentsCountingAnalyzer();
    private TokenCountingMetrics metrics;

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, defaultAnalyzer),
                "other",
                new NamedAnalyzer("other", AnalyzerScope.INDEX, otherAnalyzer),
                // A new instance for every mapper service, like a custom analyzer defined in each index's settings
                "per_index",
                new NamedAnalyzer("per_index", AnalyzerScope.INDEX, new ComponentsCountingAnalyzer())
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
     * Shards using the same analyzer share its chain, and the chain survives a mapping update while another shard
     * still uses the analyzer. Whether it survives with no other user depends on GC, so that case isn't asserted.
     */
    public void testSharesComponentsAcrossShardsAndMappingUpdates() throws IOException {
        TokenCountingMetrics metrics = metrics();
        MapperService first = createMapperService(mapping(b -> b.startObject("a").field("type", "text").endObject()));
        MapperService second = createMapperService(mapping(b -> b.startObject("a").field("type", "text").endObject()));
        Analyzer firstAnalyzer = buildIndexAnalyzer(first, metrics);
        Analyzer secondAnalyzer = buildIndexAnalyzer(second, metrics);

        TokenStream before = consume(firstAnalyzer, "a", "one two");
        assertThat(consume(secondAnalyzer, "a", "one two"), sameInstance(before));

        merge(first, mapping(b -> b.startObject("b").field("type", "text").endObject()));
        assertThat(consume(firstAnalyzer, "a", "one two"), sameInstance(before));
        assertThat(consume(firstAnalyzer, "b", "one two"), sameInstance(before));
        // Using the second shard last keeps it, and so the shared wrapper, strongly reachable until here.
        assertThat(consume(secondAnalyzer, "a", "one two"), sameInstance(before));
        assertThat(defaultAnalyzer.created.get(), equalTo(1));
    }

    /**
     * Wrappers are keyed by analyzer instance, so indices whose analyzers share a name but not an instance get separate chains.
     */
    public void testDoesNotShareComponentsBetweenAnalyzersWithTheSameName() throws IOException {
        final String fieldName = "a";
        TokenCountingMetrics metrics = metrics();
        MapperService first = createMapperService(
            mapping(b -> b.startObject(fieldName).field("type", "text").field("analyzer", "per_index").endObject())
        );
        MapperService second = createMapperService(
            mapping(b -> b.startObject(fieldName).field("type", "text").field("analyzer", "per_index").endObject())
        );

        ComponentsCountingAnalyzer firstPerIndex = getComponentsCountingAnalyzer(first, fieldName);
        ComponentsCountingAnalyzer secondPerIndex = getComponentsCountingAnalyzer(second, fieldName);
        assertThat(secondPerIndex, not(sameInstance(firstPerIndex)));

        Analyzer firstAnalyzer = buildIndexAnalyzer(first, metrics);
        Analyzer secondAnalyzer = buildIndexAnalyzer(second, metrics);
        TokenStream firstStream = consume(firstAnalyzer, "a", "one two");
        assertThat(consume(secondAnalyzer, "a", "one two"), not(sameInstance(firstStream)));
        assertThat(firstPerIndex.created.get(), equalTo(1));
        assertThat(secondPerIndex.created.get(), equalTo(1));
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
        for (boolean mappingUpdates : new boolean[] { false, true }) {
            assertConcurrentIndexing(1, mappingUpdates);
            assertConcurrentIndexing(randomIntBetween(2, 4), mappingUpdates);
        }
    }

    /**
     * Indexes concurrently into {@code shards} mapper services sharing one metrics instance, optionally while another task adds
     * fields to every shard, and asserts that each value is counted once and that chains are shared across shards.
     */
    private void assertConcurrentIndexing(int shards, boolean mappingUpdates) throws IOException {
        defaultAnalyzer.reset();
        telemetryPlugin.resetMeter();

        TokenCountingMetrics metrics = metrics();
        List<MapperService> mapperServices = new ArrayList<>();
        List<Analyzer> analyzers = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            MapperService mapperService = createMapperService(mapping(b -> {
                for (int i = 0; i < 10; i++) {
                    b.startObject("field" + i).field("type", "text").endObject();
                }
            }));
            mapperServices.add(mapperService);
            analyzers.add(buildIndexAnalyzer(mapperService, metrics));
        }
        int threads = randomIntBetween(shards * 2, shards * 8);
        int iterations = randomIntBetween(20, 100);
        int updates = randomIntBetween(5, 20);

        runInParallel(mappingUpdates ? threads + 1 : threads, t -> {
            try {
                if (t == threads) {
                    for (int u = 0; u < updates; u++) {
                        String field = "new_field" + u;
                        for (MapperService mapperService : mapperServices) {
                            merge(mapperService, mapping(b -> b.startObject(field).field("type", "text").endObject()));
                        }
                    }
                } else {
                    for (int i = 0; i < iterations; i++) {
                        // random() isn't available on these threads, so spread the work across shards deterministically
                        Analyzer analyzer = analyzers.get((t + i) % shards);
                        for (int f = 0; f < 10; f++) {
                            consume(analyzer, "field" + f, "one two three");
                        }
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });

        List<Long> counts = recordedTokenCounts();
        assertThat(counts.size(), equalTo(threads * iterations * 10));
        assertTrue(counts.stream().allMatch(c -> c == 3L));
        assertThat(defaultAnalyzer.created.get(), greaterThanOrEqualTo(1));
        if (mappingUpdates == false) {
            // One chain per thread for the shared wrapper; without node-wide sharing this could reach shards * threads.
            // A race creating duplicate wrappers is only caught probabilistically. Not asserted during mapping updates,
            // because a shard briefly drops its wrapper on each update and GC may collect it, so chains can be rebuilt.
            assertThat(defaultAnalyzer.created.get(), lessThanOrEqualTo(threads));
        } else {
            // Fields added during the updates resolve on every shard and reuse the chain shared by all shards
            telemetryPlugin.resetMeter();
            TokenStream existing = consume(analyzers.get(0), "field0", "one two");
            for (Analyzer analyzer : analyzers) {
                for (int u = 0; u < updates; u++) {
                    assertThat(consume(analyzer, "new_field" + u, "one two"), sameInstance(existing));
                }
            }
            List<Long> newCounts = recordedTokenCounts();
            assertThat(newCounts.size(), equalTo(shards * updates + 1));
            assertTrue(newCounts.stream().allMatch(c -> c == 2L));
        }
    }

    /**
     * The metrics for this test, created on first use: the histogram can only be registered once per test.
     */
    private TokenCountingMetrics metrics() {
        if (metrics == null) {
            Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
            metrics = new TokenCountingMetrics(telemetryPlugin.getTelemetryProvider(settings).getMeterRegistry());
        }
        return metrics;
    }

    private Analyzer buildIndexAnalyzer(MapperService mapperService) {
        return buildIndexAnalyzer(mapperService, metrics());
    }

    private Analyzer buildIndexAnalyzer(MapperService mapperService, TokenCountingMetrics metrics) {
        Analyzer analyzer = IndexShard.buildIndexAnalyzer(mapperService, metrics);
        assertThat(analyzer, instanceOf(TokenCountingIndexAnalyzer.class));
        return analyzer;
    }

    private static ComponentsCountingAnalyzer getComponentsCountingAnalyzer(MapperService mapperService, String field) {
        Analyzer analyzer = mapperService.indexAnalyzer(field, f -> { throw new AssertionError(); }).analyzer();
        assertThat(analyzer, instanceOf(ComponentsCountingAnalyzer.class));
        return (ComponentsCountingAnalyzer) analyzer;
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

        void reset() {
            created.set(0);
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            created.incrementAndGet();
            return new TokenStreamComponents(new StandardTokenizer());
        }
    }
}
