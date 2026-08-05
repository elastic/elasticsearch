/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.ShardResultCacheSettings;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End to end coverage of the ES|QL shard result cache: a second run of an aggregation is served from
 * {@link IndicesRequestCache} and produces the same rows, a query the verifier refuses never reaches the cache at all,
 * and anything that changes the rows a shard produces changes the key.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.esql.plugin.ShardResultCache:TRACE",
    reason = "to explain a hit, a miss or a refusal to cache"
)
public class ShardResultCacheIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "cacheable";

    /**
     * Pragmas reach the cache key, and {@link AbstractEsqlIntegTestCase#randomPragmas()} draws a fresh set per query, so
     * two runs of the same text would otherwise be two different queries as far as the cache is concerned.
     */
    private final QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);

    @Override
    protected QueryPragmas getPragmas() {
        return pragmas;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ShardResultCacheSettings.ENABLED.getKey(), true)
            // The churn gate keeps freshly written shards out, which every shard in a test is.
            .put(ShardResultCacheSettings.MIN_SHARD_IDLE_TIME.getKey(), TimeValue.ZERO)
            .build();
    }

    @Before
    public void setUpIndex() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // ESIntegTestCase randomizes this per index, and the cache honors it.
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                )
                .setMapping("host", "type=keyword", "cost", "type=long", "@timestamp", "type=date")
        );
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            docs.add(
                prepareIndex(INDEX).setSource("host", "host" + (i % 3), "cost", i, "@timestamp", "2024-01-0" + (1 + i % 5) + "T00:00:00Z")
            );
        }
        indexRandom(true, docs);
        client().admin().indices().prepareRefresh(INDEX).get();
    }

    public void testAggregationIsServedFromCacheOnSecondRun() {
        String query = "FROM " + INDEX + " | STATS total = SUM(cost) BY host | SORT host";
        List<List<Object>> first = runAndCollect(query);
        RequestCacheStats afterFirst = requestCacheStats();
        assertThat("the first run must miss on every shard", afterFirst.getMissCount(), greaterThan(0L));
        assertThat("nothing could have hit yet", afterFirst.getHitCount(), equalTo(0L));
        assertThat("the first run must have stored something", afterFirst.getMemorySizeInBytes(), greaterThan(0L));

        List<List<Object>> second = runAndCollect(query);
        assertThat(second, equalTo(first));
        RequestCacheStats afterSecond = requestCacheStats();
        assertThat("the second run must hit on every shard that missed", afterSecond.getHitCount(), equalTo(afterFirst.getMissCount()));
        assertThat("the second run must not miss", afterSecond.getMissCount(), equalTo(afterFirst.getMissCount()));
    }

    public void testFilterIsPartOfTheKey() {
        runAndCollect("FROM " + INDEX + " | STATS total = SUM(cost)");
        long missesAfterFirst = requestCacheStats().getMissCount();

        List<List<Object>> filtered = runAndCollect("FROM " + INDEX + " | WHERE cost > 10 | STATS total = SUM(cost)");
        assertThat("a different filter is a different key", requestCacheStats().getMissCount(), greaterThan(missesAfterFirst));
        assertThat(requestCacheStats().getHitCount(), equalTo(0L));

        // ... and the filtered answer is the filtered answer, not the unfiltered entry replayed.
        assertThat(filtered, equalTo(List.of(List.of(11L + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19))));
    }

    public void testNewDocumentInvalidatesTheEntry() {
        String query = "FROM " + INDEX + " | STATS count = COUNT(*)";
        assertThat(runAndCollect(query), equalTo(List.of(List.of(20L))));
        RequestCacheStats afterFirst = requestCacheStats();
        assertThat(afterFirst.getMissCount(), greaterThan(0L));

        prepareIndex(INDEX).setSource("host", "host0", "cost", 100).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertThat("a new reader must not be answered from the old reader's entry", runAndCollect(query), equalTo(List.of(List.of(21L))));
        RequestCacheStats afterRefresh = requestCacheStats();
        assertThat("the refreshed shard must miss again", afterRefresh.getMissCount(), greaterThan(afterFirst.getMissCount()));
        // Only the shard that took the new document turned its reader over, so the others still hit.
        assertThat(afterRefresh.getHitCount(), equalTo(afterFirst.getMissCount() - 1));
    }

    public void testUnsupportedShapeIsNeverCached() {
        // A row-returning query: no aggregation at the root, so the verifier refuses it and nothing is probed or stored.
        runAndCollect("FROM " + INDEX + " | KEEP host, cost | SORT cost | LIMIT 5");
        RequestCacheStats stats = requestCacheStats();
        assertThat(stats.getMissCount(), equalTo(0L));
        assertThat(stats.getHitCount(), equalTo(0L));
        assertThat(stats.getMemorySizeInBytes(), equalTo(0L));
    }

    /**
     * A runtime field whose script returns something new every time makes the shard's rows a function of when they were
     * read. ES|QL reads such a field's values without ever building a query against it, so the
     * {@code SearchExecutionContext.isCacheable()} flag that catches this on the DSL path never flips; the mapping has
     * to be asked directly.
     */
    public void testNonDeterministicRuntimeFieldIsNeverCached() throws Exception {
        String index = "noisy";
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime").startObject("noise");
        mapping.field("type", "long").startObject("script").field("source", "").field("lang", NOISE_LANG).endObject();
        mapping.endObject().endObject();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping(mapping.endObject())
        );
        indexRandom(true, List.of(prepareIndex(index).setSource("host", "host0")));

        String query = "FROM " + index + " | STATS total = SUM(noise)";
        runAndCollect(query);
        runAndCollect(query);
        RequestCacheStats stats = requestCacheStats(index);
        assertThat("a shard reading a non-deterministic field must not even be probed", stats.getMissCount(), equalTo(0L));
        assertThat(stats.getHitCount(), equalTo(0L));
        assertThat(stats.getMemorySizeInBytes(), equalTo(0L));
    }

    /**
     * The property the whole feature rests on, over every shape the verifier admits rather than over one query: what a
     * hit replays is what the same query computes with the cache turned off.
     */
    public void testAServedResultIsWhatTheQueryWouldHaveComputed() {
        for (String query : List.of(
            "FROM " + INDEX + " | STATS total = SUM(cost)",
            "FROM " + INDEX + " | STATS total = SUM(cost), hosts = COUNT_DISTINCT(host) BY host | SORT host",
            "FROM " + INDEX + " | WHERE cost > 5 | STATS count = COUNT(*), top = MAX(cost)",
            "FROM " + INDEX + " | EVAL doubled = cost * 2 | STATS total = SUM(doubled) BY host | SORT host",
            "FROM " + INDEX + " | WHERE @timestamp >= \"2024-01-03T00:00:00Z\" | STATS count = COUNT(*)"
        )) {
            updateClusterSettings(Settings.builder().put(ShardResultCacheSettings.ENABLED.getKey(), false));
            List<List<Object>> computed = runAndCollect(query);

            updateClusterSettings(Settings.builder().put(ShardResultCacheSettings.ENABLED.getKey(), true));
            runAndCollect(query);
            long hitsBefore = requestCacheStats().getHitCount();
            List<List<Object>> served = runAndCollect(query);
            assertThat(
                "the run under test has to be a hit for it to prove anything: " + query,
                requestCacheStats().getHitCount(),
                greaterThan(hitsBefore)
            );
            assertThat(query, served, equalTo(computed));
        }
        updateClusterSettings(Settings.builder().putNull(ShardResultCacheSettings.ENABLED.getKey()));
    }

    private List<List<Object>> runAndCollect(String query) {
        try (EsqlQueryResponse response = run(query)) {
            return getValuesList(response);
        }
    }

    private RequestCacheStats requestCacheStats() {
        return requestCacheStats(INDEX);
    }

    private RequestCacheStats requestCacheStats(String index) {
        return client().admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache();
    }

    private static final String NOISE_LANG = "shard-cache-noise";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(NoisyRuntimeFieldPlugin.class);
        return plugins;
    }

    /** A {@code long} runtime field that emits a fresh random value per document and admits to being non-deterministic. */
    public static class NoisyRuntimeFieldPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return NOISE_LANG;
                }

                @Override
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    if (context != LongFieldScript.CONTEXT) {
                        throw new IllegalArgumentException("unsupported context " + context);
                    }
                    // Safe: context == LongFieldScript.CONTEXT guarantees FactoryType == LongFieldScript.Factory.
                    @SuppressWarnings("unchecked")
                    FactoryType result = (FactoryType) new LongFieldScript.Factory() {
                        @Override
                        public boolean isResultDeterministic() {
                            return false;
                        }

                        @Override
                        public LongFieldScript.LeafFactory newFactory(
                            String fieldName,
                            Map<String, Object> params,
                            SearchLookup searchLookup,
                            OnScriptError onScriptError
                        ) {
                            return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                @Override
                                public void execute() {
                                    emit(randomLong());
                                }
                            };
                        }
                    };
                    return result;
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }
}
