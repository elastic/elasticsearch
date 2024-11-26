/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.recovery.metering.RecoveryMetricsCollector;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RecoveryMetricsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    public void testRecoveryMetricPublicationOnIndexingShardRelocation() throws Exception {
        startMasterOnlyNode();
        var indexingNode1 = startIndexNode();
        var indexingNode2 = startIndexNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // ensure that index shard is allocated on `indexingNode1` and not on `indexingNode2`
        assertAcked(
            admin().indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexingNode2))
        );

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, indexingNode2)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        // trigger primary relocation from `indexingNode1` to `indexingNode2`
        // hence start recovery of the shard on a new node
        assertAcked(
            admin().indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexingNode1))
        );

        assertBusy(() -> {
            final List<Measurement> measurements = plugin.getLongHistogramMeasurement(RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC);
            assertFalse("Total recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThan(0L));
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("PEER"));
        });

        assertBusy(() -> {
            final List<Measurement> measurements = plugin.getLongHistogramMeasurement(RecoveryMetricsCollector.RECOVERY_INDEX_TIME_METRIC);
            assertFalse("Index recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("PEER"));
        });

        assertBusy(() -> {
            final List<Measurement> measurements = plugin.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TRANSLOG_TIME_METRIC
            );
            assertFalse("Translog recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("PEER"));
        });
    }

    public void testRecoveryMetricPublicationOnIndexingShardStartedAndThenRecovered() throws Exception {
        startMasterOnlyNode();
        var indexingNode1 = startIndexNode();

        final TestTelemetryPlugin pluginOnNode1 = internalCluster().getInstance(PluginsService.class, indexingNode1)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        pluginOnNode1.resetMeter();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // check that a brand new shard went thru `EMPTY_STORE` recovery type
        assertBusy(() -> {
            final List<Measurement> measurements = pluginOnNode1.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC
            );
            assertFalse("Total recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("EMPTY_STORE"));
        });

        // ensure that index shard is allocated on `indexingNode1` only
        assertAcked(
            admin().indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexingNode1))
        );

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        internalCluster().stopNode(indexingNode1);

        // master only node exists
        assertBusy(() -> assertThat(internalCluster().size(), equalTo(1)));

        var indexingNode2 = startIndexNode();

        final TestTelemetryPlugin pluginOnNode2 = internalCluster().getInstance(PluginsService.class, indexingNode2)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        pluginOnNode2.resetMeter();

        // start recovery of the shard on a new node
        assertAcked(
            admin().indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name"))
        );

        assertBusy(() -> {
            final List<Measurement> measurements = pluginOnNode2.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC
            );
            assertFalse("Total recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThan(0L));
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("EXISTING_STORE"));
        });

        assertBusy(() -> {
            final List<Measurement> measurements = pluginOnNode2.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_INDEX_TIME_METRIC
            );
            assertFalse("Index recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("EXISTING_STORE"));
        });

        assertBusy(() -> {
            final List<Measurement> measurements = pluginOnNode2.getLongHistogramMeasurement(
                RecoveryMetricsCollector.RECOVERY_TRANSLOG_TIME_METRIC
            );
            assertFalse("Translog recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat(metric.attributes().get("primary"), equalTo(true));
            assertThat(metric.attributes().get("recovery_type"), equalTo("EXISTING_STORE"));
        });
    }

    public void testRecoveryMetricPublicationOnSearchingShardStart() throws Exception {
        startMasterOnlyNode();
        startIndexNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        var searchNode = startSearchNode();

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, searchNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        // initiate allocation and shard recovery on `searchNode`
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));

        assertBusy(() -> {
            final List<Measurement> measurements = plugin.getLongHistogramMeasurement(RecoveryMetricsCollector.RECOVERY_TOTAL_TIME_METRIC);
            assertFalse("Total recovery time metric is not recorded", measurements.isEmpty());
            assertThat(measurements.size(), equalTo(1));
            final Measurement metric = measurements.get(0);
            assertThat(metric.value().longValue(), greaterThan(0L));
            assertThat(metric.attributes().get("primary"), equalTo(false));
            assertThat(metric.attributes().get("recovery_type"), equalTo("PEER"));
        });
    }

    public void testRecoveryMetricPublicationBytesReadToCache() {
        startMasterOnlyNode();
        // in scenarios where cache is disabled (see AbstractStatelessIntegTestCase#settingsForRoles)
        // nothing is copied to cache and so metric in SharedBlobCacheWarmingService is not incremented
        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(2L * PAGE_SIZE).getStringRep())
            .build();

        var indexingNode1 = startIndexNode(cacheSettings);
        var indexingNode2 = startIndexNode(cacheSettings);

        var indexName = randomIdentifier();
        // ensure that index shard is allocated on `indexingNode1` and not on `indexingNode2`
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings(1, 0).build())
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexingNode2)
                .build()
        );
        ensureGreen(indexName);

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        var plugin = internalCluster().getInstance(PluginsService.class, indexingNode2)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        // trigger primary relocation from `indexingNode1` to `indexingNode2`
        // hence start recovery of the shard on a new node
        assertAcked(
            admin().indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexingNode1))
        );

        ensureGreen(indexName);

        var recoveriesMetric = getSingleRecordedMetric(
            plugin::getLongCounterMeasurement,
            RecoveryMetricsCollector.RECOVERY_TOTAL_COUNT_METRIC
        );
        assertThat(recoveriesMetric.getLong(), greaterThan(0L));

        var warmedFromObjectStoreMetric = getSingleRecordedMetric(
            plugin::getLongCounterMeasurement,
            RecoveryMetricsCollector.RECOVERY_BYTES_WARMED_FROM_OBJECT_STORE_METRIC
        );
        assertMetricAttributes(warmedFromObjectStoreMetric, true);

        var readFromObjectStoreMetric = getSingleRecordedMetric(
            plugin::getLongCounterMeasurement,
            RecoveryMetricsCollector.RECOVERY_BYTES_READ_FROM_OBJECT_STORE_METRIC
        );
        assertMetricAttributes(readFromObjectStoreMetric, true);

        assertThat(
            "No bytes read or warmed from object store",
            warmedFromObjectStoreMetric.getLong() + readFromObjectStoreMetric.getLong(),
            greaterThan(0L)
        );

        {
            final List<Measurement> measurements = plugin.getLongCounterMeasurement(
                SharedBlobCacheWarmingService.BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC
            );
            // One from IndexShardCacheWarmer and the other from StatelessIndexEventListener
            assertThat(measurements.size(), equalTo(2));
            long totalBytesWarmed = 0;
            for (final Measurement metric : measurements) {
                final long bytesWarmed = metric.getLong();
                assertThat(bytesWarmed, greaterThanOrEqualTo(0L));
                totalBytesWarmed += bytesWarmed;
                assertMetricAttributes(metric, true);
            }
            assertThat(totalBytesWarmed, greaterThan(0L));
        }
    }

    public void testRecoveryMetricPublicationBytesReadToCacheFromIndexing() {
        startMasterOnlyNode();
        // prevent implicit refreshes
        startIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
                .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.MAX_VALUE)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .build()
        );

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        // have commits both uploaded to object store and pending in vbcc

        var indexShard = findIndexShard(indexName);
        var indexEngine = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull());
        var commitService = indexEngine.getStatelessCommitService();

        indexDocs(indexName, randomIntBetween(10, 20));
        flush(indexName);   // upload via flush
        assertThat(commitService.getCurrentVirtualBcc(indexShard.shardId()), nullValue());

        indexDocs(indexName, randomIntBetween(10, 20));
        // create vbcc but do not upload: upload is disabled and index engine flush does not do upload since it is originated from refresh
        refresh(indexName);
        assertThat(commitService.getCurrentVirtualBcc(indexShard.shardId()), notNullValue());
        ensureGreen(indexName);

        var searchNode = startSearchNode();

        var plugin = internalCluster().getInstance(PluginsService.class, searchNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        // initiate allocation and shard recovery on `searchNode`
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);

        boolean warmedBytes;
        boolean readBytes;
        {
            var metric = getSingleRecordedMetric(
                plugin::getLongCounterMeasurement,
                RecoveryMetricsCollector.RECOVERY_BYTES_WARMED_FROM_INDEXING_METRIC
            );
            warmedBytes = metric.getLong() > 0;
            assertMetricAttributes(metric, false);
        }
        {
            var metric = getSingleRecordedMetric(
                plugin::getLongCounterMeasurement,
                RecoveryMetricsCollector.RECOVERY_BYTES_READ_FROM_INDEXING_METRIC
            );
            readBytes = metric.getLong() > 0;
            assertMetricAttributes(metric, false);
        }
        assertThat("No bytes read or warmed from indexing", warmedBytes || readBytes, equalTo(true));
    }

    private static Measurement getSingleRecordedMetric(Function<String, List<Measurement>> metricGetter, String name) {
        final List<Measurement> measurements = metricGetter.apply(name);
        assertFalse("Metric is not recorded", measurements.isEmpty());
        assertThat(measurements.size(), equalTo(1));
        return measurements.get(0);
    }

    private void assertMetricAttributes(Measurement metric, boolean isPrimary) {
        assertThat(metric.attributes().get("primary"), equalTo(isPrimary));
    }
}
