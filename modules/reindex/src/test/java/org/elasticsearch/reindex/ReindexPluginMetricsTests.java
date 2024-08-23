/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexPluginMetricsTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, TestTelemetryPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return new ReindexRequestBuilder(client());
    }

    protected UpdateByQueryRequestBuilder updateByQuery() {
        return new UpdateByQueryRequestBuilder(client());
    }

    protected DeleteByQueryRequestBuilder deleteByQuery() {
        return new DeleteByQueryRequestBuilder(client());
    }

    public static BulkIndexByScrollResponseMatcher matcher() {
        return new BulkIndexByScrollResponseMatcher();
    }

    protected TestTelemetryPlugin getTestTelemetryPlugin() {
        return (internalCluster().getInstance(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0));
    }

    public void testFiltering() throws Exception {
        internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "a"),
            prepareIndex("source").setId("3").setSource("foo", "b"),
            prepareIndex("source").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        TestTelemetryPlugin testTelemetryPlugin = getTestTelemetryPlugin();
        // testTelemetryPlugin.resetMeter();
        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        assertThat(copy.get(), matcher().created(4));
        // testTelemetryPlugin.collect();
        assertHitCount(prepareSearch("dest").setSize(0), 4);
        List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
        assertThat(measurements.size(), equalTo(1));

        // Now none of them
        createIndex("none");
        copy = reindex().source("source").destination("none").filter(termQuery("foo", "no_match")).refresh(true);
        assertThat(copy.get(), matcher().created(0));
        assertHitCount(prepareSearch("none").setSize(0), 0);
        // testTelemetryPlugin.collect();
        measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
        assertThat(measurements.size(), equalTo(2));

        // Now half of them
        copy = reindex().source("source").destination("dest_half").filter(termQuery("foo", "a")).refresh(true);
        assertThat(copy.get(), matcher().created(2));
        assertHitCount(prepareSearch("dest_half").setSize(0), 2);
        // testTelemetryPlugin.collect();
        measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
        assertThat(measurements.size(), equalTo(3));

        // Limit with maxDocs
        copy = reindex().source("source").destination("dest_size_one").maxDocs(1).refresh(true);
        assertThat(copy.get(), matcher().created(1));
        assertHitCount(prepareSearch("dest_size_one").setSize(0), 1);
    }
}
