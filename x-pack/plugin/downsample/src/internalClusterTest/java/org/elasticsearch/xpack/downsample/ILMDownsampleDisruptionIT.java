/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomInterval;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 4)
public class ILMDownsampleDisruptionIT extends DownsamplingIntegTestCase {
    private static final Logger logger = LogManager.getLogger(ILMDownsampleDisruptionIT.class);
    private static final String POLICY_NAME = "mypolicy";
    public static final int DOC_COUNT = 10_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            Downsample.class,
            AggregateMetricMapperPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            IndexLifecycle.class,
            Ccr.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder nodeSettings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        nodeSettings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");

        // This is necessary to prevent ILM installing a lifecycle policy, these tests assume a blank slate
        nodeSettings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        return nodeSettings.build();
    }

    public void setup(final String sourceIndex, int numOfShards, int numOfReplicas, long startTime) throws IOException {
        final Settings.Builder settings = indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_KEYWORD))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2022-01-08T23:40:53.384Z");

        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean());
        }

        final XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        mapping.startObject(FIELD_DIMENSION_KEYWORD).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_LONG).field("type", "long").field("time_series_dimension", true).endObject();

        mapping.startObject(FIELD_METRIC_COUNTER_DOUBLE)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        mapping.endObject().endObject().endObject();
        assertAcked(indicesAdmin().prepareCreate(sourceIndex).setSettings(settings.build()).setMapping(mapping).get());

        Map<String, Phase> phases = new HashMap<>();
        phases.put(
            "warm",
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of("downsample", new org.elasticsearch.xpack.core.ilm.DownsampleAction(DateHistogramInterval.HOUR, null))
            )
        );
        LifecyclePolicy policy = new LifecyclePolicy(POLICY_NAME, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());
    }

    public void testILMDownsampleRollingRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        long startTime = LocalDateTime.parse("1993-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        setup(sourceIndex, 1, 0, startTime);
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            final String ts = randomDateForInterval(config.getInterval(), startTime);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, ts)
                    .field(FIELD_DIMENSION_KEYWORD, randomFrom(dimensionValues))
                    .field(FIELD_DIMENSION_LONG, randomIntBetween(1, 10))
                    .field(FIELD_METRIC_COUNTER_DOUBLE, counterValue)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        int indexedDocs = bulkIndex(sourceIndex, sourceSupplier, DOC_COUNT);

        cluster.rollingRestart(new InternalTestCluster.RestartCallback());

        final String targetIndex = "downsample-1h-" + sourceIndex;
        startDownsampleTaskViaIlm(sourceIndex, targetIndex);
        assertBusy(() -> assertTargetIndex(cluster, targetIndex, indexedDocs));
        ensureGreen(targetIndex);
    }

    private void startDownsampleTaskViaIlm(String sourceIndex, String targetIndex) throws Exception {
        var request = new UpdateSettingsRequest(sourceIndex).settings(
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, POLICY_NAME)
        );
        // Updating index.lifecycle.name setting may fail due to the rolling restart itself,
        // we need to attempt it in a assertBusy(...)
        assertBusy(() -> {
            try {
                if (indexExists(sourceIndex) == false) {
                    logger.info("The source index [{}] no longer exists, downsampling likely completed", sourceIndex);
                    return;
                }
                client().admin().indices().updateSettings(request).actionGet(TimeValue.timeValueSeconds(10));
            } catch (Exception e) {
                logger.warn(() -> format("encountered failure while updating [%s] index's ilm policy", sourceIndex), e);
                throw new AssertionError(e);
            }
        }, 1, TimeUnit.MINUTES);
        awaitIndexExists(targetIndex, TimeValue.timeValueSeconds(60));
        assertBusy(() -> {
            var getSettingsResponse = client().admin()
                .indices()
                .getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(targetIndex))
                .actionGet();
            assertThat(getSettingsResponse.getSetting(targetIndex, IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey()), equalTo("success"));
        }, 60, TimeUnit.SECONDS);
    }

    private void assertTargetIndex(final InternalTestCluster cluster, final String targetIndex, int indexedDocs) {
        final GetIndexResponse getIndexResponse = cluster.client()
            .admin()
            .indices()
            .getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(targetIndex))
            .actionGet();
        assertEquals(1, getIndexResponse.indices().length);
        assertResponse(
            cluster.client()
                .prepareSearch(targetIndex)
                .setQuery(new MatchAllQueryBuilder())
                .setSize(Math.min(DOC_COUNT, indexedDocs))
                .setTrackTotalHitsUpTo(Integer.MAX_VALUE),
            targetIndexSearch -> {
                assertTrue(targetIndexSearch.getHits().getHits().length > 0);
            }
        );
    }
}
