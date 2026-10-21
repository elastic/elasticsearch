/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.bulk.PreResolvedUpdates;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.hamcrest.Matchers.greaterThan;

public class BulkUpdateStoredFieldsPrefetchIT extends AbstractStatelessPluginIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testStoredFieldsPrefetchedBeforeUpdateExecution() throws Exception {
        // Small region size so each document's stored-fields chunk spans multiple regions, making the
        // prefetch observable as multiple Fetched outcomes rather than a single AlreadyCached region.
        final var regionSizeInBytes = 4 * SharedBytes.PAGE_SIZE;
        startMasterOnlyNode();
        String indexNode = startIndexNode(
            Settings.builder()
                .put(PreResolvedUpdates.PRE_RESOLVE_BULK_UPDATES.getKey(), true)
                .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
                .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
                .put(
                    SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.ofBytes(256L * regionSizeInBytes).getStringRep()
                )
                .build()
        );
        ensureStableCluster(2);

        String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var initialBulk = client().prepareBulk();
        var numDocsToUpdate = randomIntBetween(5, 10);
        List<String> docIds = new ArrayList<>(numDocsToUpdate);
        // Index large documents. Each document's stored-fields chunk will be large enough to span
        // multiple blob-cache regions after the flush uploads segments to the object store.
        var fieldLength = regionSizeInBytes * randomIntBetween(2, 4);
        for (int i = 0; i < numDocsToUpdate; i++) {
            String id = UUIDs.randomBase64UUID();
            docIds.add(id);
            initialBulk.add(
                client().prepareIndex(indexName).setId(id).setSource(Map.of("content", randomAlphanumericOfLength(fieldLength)))
            );
        }
        assertNoFailures(initialBulk.get());

        // Flush uploads segments to the object store and deletes local copies. After this point,
        // the index engine reads stored fields through the blob cache.
        flush(indexName);
        refresh(indexName);

        // Start with an empty blob cache so every stored-fields region is cold.
        var primaryShard = findIndexShard(indexName);
        BlobStoreCacheDirectory blobDir = BlobStoreCacheDirectory.unwrapDirectory(primaryShard.store().directory());
        getCacheService(blobDir).forceEvict(key -> true);

        TestTelemetryPlugin telemetry = getTelemetryPlugin(indexNode);
        telemetry.resetMeter();

        // Block the auto-put-mapping acknowledgement on the master. The bulk's item-0 IndexRequest
        // introduces a new field, triggering a mapping update that stalls the shard thread. By the
        // time this stall begins, PreResolvedUpdates.resolve() has already run for all UpdateRequests
        // (pre-resolve + prefetch happen before any item is executed).
        CountDownLatch mappingUpdateReached = new CountDownLatch(1);
        CountDownLatch mappingUpdateRelease = new CountDownLatch(1);
        MockTransportService masterTransport = MockTransportService.getInstance(internalCluster().getMasterName());
        masterTransport.addRequestHandlingBehavior(TransportAutoPutMappingAction.TYPE.name(), (handler, request, channel, task) -> {
            mappingUpdateReached.countDown();
            safeAwait(mappingUpdateRelease);
            handler.messageReceived(request, channel, task);
        });

        var bulk = client().prepareBulk();
        // The new field should trigger a dynamic mapping update that will be blocked
        bulk.add(client().prepareIndex(indexName).setSource((Map.of("content", "x", "new_dynamic_field", "trigger"))));
        for (String id : docIds) {
            bulk.add(
                client().prepareUpdate(indexName, id).setDoc((Map.of("content", randomAlphanumericOfLength(randomIntBetween(10, 20)))))
            );
        }
        var bulkFuture = bulk.execute();

        safeAwait(mappingUpdateReached);

        // While the shard thread is blocked, the async object-store downloads scheduled by prefetch()
        // run in background threads. Wait until at least one completes (PrefetchResult.Fetched > 0).
        try {
            assertBusy(() -> {
                long fetched = telemetry.getLongCounterMeasurement(BlobCacheMetrics.BLOB_CACHE_PREFETCH_TOTAL)
                    .stream()
                    .filter(
                        m -> BlobCacheMetrics.PrefetchResult.Fetched.name()
                            .equals(m.attributes().get(BlobCacheMetrics.PREFETCH_RESULT_ATTRIBUTE_KEY))
                    )
                    .mapToLong(Measurement::getLong)
                    .sum();
                assertThat(
                    "at least one stored-fields region should have been asynchronously fetched by the prefetch() call",
                    fetched,
                    greaterThan(0L)
                );
            });
        } finally {
            // Always unblock the master so the bulk can complete and the cluster stays healthy.
            mappingUpdateRelease.countDown();
            masterTransport.clearAllRules();
        }

        assertNoFailures(safeGet(bulkFuture));
    }

    public void testNoPrefetchForNonStoredSourceMode() {
        startMasterOnlyNode();
        String indexNode = startIndexNode(Settings.builder().put(PreResolvedUpdates.PRE_RESOLVE_BULK_UPDATES.getKey(), true).build());
        ensureStableCluster(2);

        String indexName = randomIdentifier();
        // For SYNTHETIC source, Lucene calls IndexInput.prefetch() during doc-value reads used to
        // reconstruct the source, so the metric is non-zero even though our pre-resolution path is
        // skipped.
        Settings.Builder indexSettingsBuilder = indexSettings(1, 0).put(
            IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(),
            SourceFieldMapper.Mode.COLUMNAR_STORED.name()
        ).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName());
        createIndex(indexName, indexSettingsBuilder.build());
        ensureGreen(indexName);

        var initialBulk = client().prepareBulk();
        var numDocsToUpdate = randomIntBetween(5, 10);
        List<String> docIds = new ArrayList<>(numDocsToUpdate);
        for (int i = 0; i < numDocsToUpdate; i++) {
            String id = UUIDs.randomBase64UUID();
            docIds.add(id);
            initialBulk.add(client().prepareIndex(indexName).setId(id).setSource(Map.of("value", randomInt())));
        }
        assertNoFailures(initialBulk.get());
        flush(indexName);
        refresh(indexName);

        TestTelemetryPlugin telemetry = getTelemetryPlugin(indexNode);
        telemetry.resetMeter();

        var bulk = client().prepareBulk();
        for (String id : docIds) {
            bulk.add(client().prepareUpdate(indexName, id).setDoc(Map.of("value", randomInt())));
        }
        assertNoFailures(safeGet(bulk.execute()));

        long totalPrefetches = telemetry.getLongCounterMeasurement(BlobCacheMetrics.BLOB_CACHE_PREFETCH_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertEquals(
            "no stored-fields prefetch should occur for source mode " + SourceFieldMapper.Mode.COLUMNAR_STORED.name(),
            0L,
            totalPrefetches
        );
    }
}
