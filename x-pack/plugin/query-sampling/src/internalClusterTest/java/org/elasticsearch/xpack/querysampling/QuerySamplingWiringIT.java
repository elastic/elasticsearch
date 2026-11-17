/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureSink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The wiring that the unit tests builds by hand: the server loads the sink provider of the plugin through SPI, the query
 * interceptor hands a real kNN search to the sink, and the sink hands it through the queue to the sampling pipeline.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class QuerySamplingWiringIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(KnnQuerySamplingPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(QuerySamplingSettings.ENABLED.getKey(), true)
            .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 1.0)
            .build();
    }

    public void testAKnnSearchReachesTheSamplingPipelineAndTheSwitchTurnsItOff() throws Exception {
        assumeTrue("the plugin does nothing without its feature flag", KnnQuerySamplingPlugin.QUERY_SAMPLING_FEATURE_FLAG.isEnabled());
        createIndexWithVectors();
        QueryCaptureSink sink = plugin().captureSink();
        assertThat("the plugin builds its sink, and the server found it through SPI to give it searches", sink, notNullValue());

        knnSearch();
        assertThat(sink.knnSearches(), equalTo(1L));
        assertThat("every search is captured at a capture rate of 1", sink.captured(), equalTo(1L));
        assertBusy(() -> assertThat("the capture went through the queue to the pipeline", pipelineTasksCompleted(), equalTo(1L)));

        updateClusterSettings(Settings.builder().put(QuerySamplingSettings.ENABLED.getKey(), false));
        knnSearch();
        assertThat("switched off, the search is not even looked at", sink.knnSearches(), equalTo(1L));
        assertThat(pipelineTasksCompleted(), equalTo(1L));
    }

    private void createIndexWithVectors() {
        assertAcked(
            prepareCreate("vectors").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("""
                    { "properties": { "vec": { "type": "dense_vector", "dims": 4, "index": true, "similarity": "l2_norm" } } }""")
        );
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            docs.add(prepareIndex("vectors").setId(Integer.toString(i)).setSource("vec", new float[] { i, 1f, 0f, 0f }));
        }
        indexRandom(true, docs);
    }

    private void knnSearch() {
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", new float[] { 1f, 1f, 0f, 0f }, 3, 10, null, null, null);
        assertResponse(
            // the test client randomly gives searches a preference, which the sink does not capture, as it can change which copies answer
            prepareSearch("vectors").setPreference(null).setKnnSearch(List.of(knn)).setSize(3),
            response -> assertThat(response.getHits().getHits().length, equalTo(3))
        );
    }

    private KnnQuerySamplingPlugin plugin() {
        return internalCluster().getInstance(PluginsService.class).filterPlugins(KnnQuerySamplingPlugin.class).findFirst().orElseThrow();
    }

    private long pipelineTasksCompleted() {
        for (ThreadPoolStats.Stats stats : internalCluster().getInstance(ThreadPool.class).stats()) {
            if (stats.name().equals(KnnQuerySamplingPlugin.THREAD_POOL_NAME)) {
                return stats.completed();
            }
        }
        throw new AssertionError("no thread pool [" + KnnQuerySamplingPlugin.THREAD_POOL_NAME + "]");
    }
}
