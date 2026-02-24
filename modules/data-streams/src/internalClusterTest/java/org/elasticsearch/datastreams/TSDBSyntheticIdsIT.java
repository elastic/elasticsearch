/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageTestUtils;
import org.elasticsearch.action.admin.indices.diskusage.IndexDiskUsageStats;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.elasticsearch.index.shard.IndexShardTestCase.getTranslog;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertCheckedResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test suite for time series indices that use synthetic ids for documents.
 * <p>
 * Synthetic _id fields are not indexed in Lucene, instead they are generated on demand by concatenating the values of two other fields of
 * the document (typically the {@code @timestamp} and {@code _tsid} fields).
 * </p>
 */
@LuceneTestCase.SuppressCodecs("*") // requires codecs used in production only
public class TSDBSyntheticIdsIT extends ESIntegTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern(STRICT_DATE_OPTIONAL_TIME.getName());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testInvalidIndexMode() {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var indexName = randomIdentifier();
        var randomNonTsdbIndexMode = randomValueOtherThan(IndexMode.TIME_SERIES, () -> randomFrom(IndexMode.values()));

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                indexName,
                indexSettings(1, 0).put(IndexSettings.MODE.getKey(), randomNonTsdbIndexMode)
                    .put(IndexSettings.SYNTHETIC_ID.getKey(), true)
                    .build()
            )
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "The setting ["
                    + IndexSettings.SYNTHETIC_ID.getKey()
                    + "] is only permitted when [index.mode] is set to [TIME_SERIES]. Current mode: ["
                    + randomNonTsdbIndexMode.getName().toUpperCase(Locale.ROOT)
                    + "]."
            )
        );
    }

    public void testInvalidCodec() {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var indexName = randomIdentifier();
        internalCluster().startDataOnlyNode();
        var randomNonDefaultCodec = randomFrom(
            CodecService.BEST_COMPRESSION_CODEC,
            CodecService.LEGACY_DEFAULT_CODEC,
            CodecService.BEST_COMPRESSION_CODEC,
            CodecService.LUCENE_DEFAULT_CODEC
        );

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                indexName,
                indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .put("index.routing_path", "hostname")
                    .put(IndexSettings.SYNTHETIC_ID.getKey(), true)
                    .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), randomNonDefaultCodec)
                    .build()
            )
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "The setting ["
                    + IndexSettings.SYNTHETIC_ID.getKey()
                    + "] is only permitted when [index.codec] is set to [default]. Current mode: ["
                    + randomNonDefaultCodec
                    + "]."
            )
        );
    }

    public void testSyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, randomIntBetween(1, 5), 0);

        final var docs = new HashMap<String, String>();
        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();
        logger.info("timestamp is " + timestamp);

        // Index 10 docs in datastream
        //
        // For convenience, the metric value maps the index in the bulk response items
        var results = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9)
        );

        // Verify that documents are created
        for (var result : results) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            docs.put(result.getId(), result.getIndex());
        }
        final int initialNumberOfDocs = results.length;

        enum Operation {
            FLUSH,
            REFRESH,
            NONE
        }

        // Random flush or refresh or nothing, so that the next GETs are executed on flushed segments or in memory segments.
        switch (randomFrom(Operation.values())) {
            case FLUSH:
                flush(dataStreamName);
                break;
            case REFRESH:
                refresh(dataStreamName);
                break;
            case NONE:
            default:
                break;
        }

        // Get by synthetic _id
        var randomDocs = randomSubsetOf(randomIntBetween(0, results.length), results);
        for (var doc : randomDocs) {
            boolean fetchSource = randomBoolean();
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId()).setFetchSource(fetchSource).get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            if (fetchSource) {
                var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
                assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(doc.getItemId()));
            }
        }

        // Random flush or refresh or nothing, so that the next DELETEs are executed on flushed segments or in memory segments.
        switch (randomFrom(Operation.values())) {
            case FLUSH:
                flush(dataStreamName);
                break;
            case REFRESH:
                refresh(dataStreamName);
                break;
            case NONE:
            default:
                break;
        }

        // Delete by synthetic _id
        var deletedDocs = deleteRandomDocuments(docs);

        // Index more random docs
        if (randomBoolean()) {
            int nbDocs = randomIntBetween(1, 100);
            final var arrayOfDocs = new XContentBuilder[nbDocs];

            var t = timestamp.plus(4, unit); // t + 4s, no overlap with previous docs
            while (nbDocs > 0) {
                var hosts = randomSubsetOf(List.of("vm-dev01", "vm-dev02", "vm-dev03"));
                for (var host : hosts) {
                    if (--nbDocs < 0) {
                        break;
                    }
                    arrayOfDocs[nbDocs] = document(t, host, "cpu-load", randomInt(10));
                }
                // always use seconds, otherwise the doc might fell outside of the timestamps window of the datastream
                t = t.plus(1, ChronoUnit.SECONDS);
            }

            results = createDocuments(dataStreamName, arrayOfDocs);

            // Verify that documents are created
            for (var result : results) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertThat(result.getVersion(), equalTo(1L));
                docs.put(result.getId(), result.getIndex());
            }
        }

        refresh(dataStreamName);

        assertCheckedResponse(client().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(100), searchResponse -> {
            assertHitCount(searchResponse, docs.size() - deletedDocs.size());

            // Verify that search response does not contain deleted docs
            for (var searchHit : searchResponse.getHits()) {
                assertThat(
                    "Document with id [" + searchHit.getId() + "] is deleted",
                    deletedDocs.contains(searchHit.getId()),
                    equalTo(false)
                );
            }
        });

        // Search by synthetic _id
        var otherDocs = randomSubsetOf(Sets.difference(docs.keySet(), Sets.newHashSet(deletedDocs)));
        assertSearchById(otherDocs, docs);

        if (randomBoolean()) {
            flush(dataStreamName);
        }
        if (randomBoolean()) {
            forceMerge();
        }

        if (randomBoolean()) {
            logger.info("--> restarting the cluster");
            internalCluster().rollingRestart(new InternalTestCluster.RestartCallback());
        } else {
            // Move all the shards to a new node to force relocations
            var newNodeName = internalCluster().startDataOnlyNode();
            logger.info("--> relocating all shards to {}", newNodeName);

            var dataStream = client().admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .getMetadata()
                .getProject(ProjectId.DEFAULT)
                .dataStreams()
                .get(dataStreamName);
            assertThat(dataStream, notNullValue());
            for (Index index : dataStream.getIndices()) {
                updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", newNodeName), index.getName());
                ensureGreen(index.getName());
            }
        }

        // After the restart/relocation we'll try to index the same set of initial metrics
        // to ensure that the version lookup works as expected. Additionally, some of the
        // docs might have been deleted, so those should go through without issues.
        var bulkResponses = createDocumentsWithoutValidatingTheResponse(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9)
        );

        var successfulRequests = Arrays.stream(bulkResponses).filter(response -> response.isFailed() == false).toList();
        assertThat(successfulRequests, hasSize(deletedDocs.size()));

        var failedRequests = Arrays.stream(bulkResponses).filter(BulkItemResponse::isFailed).toList();
        assertThat(failedRequests, hasSize(initialNumberOfDocs - deletedDocs.size()));
        for (BulkItemResponse failedRequest : failedRequests) {
            assertThat(failedRequest.getFailure().getCause(), is(instanceOf(VersionConflictEngineException.class)));
        }

        // Check that synthetic _id field have no postings on disk but has bloom filter usage
        var indices = new HashSet<>(docs.values());
        for (var index : indices) {
            var diskUsage = diskUsage(index);
            var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
            assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
            assertThat("_id field should have bloom filter usage", diskUsageIdField.getBloomFilterBytes(), greaterThan(0L));
        }
    }

    public void testGetFromTranslogBySyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, 1, 0);

        final var docs = new HashMap<String, String>();
        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();

        // Index 5 docs in datastream
        //
        // For convenience, the metric value maps the index in the bulk response items
        var results = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4)
        );

        // Verify that documents are created
        for (var result : results) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            docs.put(result.getId(), result.getIndex());
        }

        // Get by synthetic _id
        //
        // The documents are in memory buffers: the first GET will trigger the refresh of the internal reader
        // (see InternalEngine.REAL_TIME_GET_REFRESH_SOURCE) to have an up-to-date searcher to resolve documents ids and versions. It will
        // also enable the tracking of the locations of documents in the translog (see InternalEngine.trackTranslogLocation) so that next
        // GETs will be resolved using the translog.
        var randomDocs = randomSubsetOf(randomIntBetween(1, results.length), results);
        for (var doc : randomDocs) {
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId()).setRealtime(true).setFetchSource(true).execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
            assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(doc.getItemId()));
        }

        int metricOffset = results.length;

        // Index 5 more docs
        results = createDocuments(
            dataStreamName,
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", metricOffset),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", metricOffset + 1),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", metricOffset + 2),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", metricOffset + 3),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", metricOffset + 4)
        );

        // Verify that documents are created
        for (var result : results) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            docs.put(result.getId(), result.getIndex());
        }

        // Get by synthetic _id
        //
        // Documents ids and versions are resolved using the translog. Here we exercise the get-from-translog (that uses the
        // TranslogDirectoryReader) and VersionsAndSeqNoResolver.loadDocIdAndVersionUncached paths.
        randomDocs = randomSubsetOf(randomIntBetween(1, results.length), results);
        for (var doc : randomDocs) {
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId()).setRealtime(true).setFetchSource(true).execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
            assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(metricOffset + doc.getItemId()));
        }

        flushAndRefresh(dataStreamName);

        // Get by synthetic _id
        //
        // Here we exercise the get-from-searcher and VersionsAndSeqNoResolver.timeSeriesLoadDocIdAndVersion paths.
        randomDocs = randomSubsetOf(randomIntBetween(1, results.length), results);
        for (var doc : randomDocs) {
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId())
                .setRealtime(randomBoolean())
                .setFetchSource(true)
                .execute()
                .actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
            assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(metricOffset + doc.getItemId()));
        }

        // Check that synthetic _id field have no postings on disk but has bloom filter usage
        var indices = new HashSet<>(docs.values());
        for (var index : indices) {
            var diskUsage = diskUsage(index);
            var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
            assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
            assertThat("_id field should have bloom filter usage", diskUsageIdField.getBloomFilterBytes(), greaterThan(0L));
        }

        assertHitCount(client().prepareSearch(dataStreamName).setSize(0), 10L);
    }

    public void testRecoveredOperations() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);

        // ensure a couple of nodes to have some operations coordinated
        internalCluster().ensureAtLeastNumDataNodes(2);

        final var dataStreamName = randomIdentifier();
        final int numShards = randomIntBetween(1, 10);
        putDataStreamTemplate(dataStreamName, numShards, 0);

        final var docsIndices = new HashSet<String>();
        final var docsIndicesById = new HashMap<String, String>();
        final var docsIdsBySeqNoAndShardId = new HashMap<ShardId, Map<Long, String>>();

        var timestamp = Instant.now();
        // Use `timestamp = Instant.ofEpochMilli(epoch)` to set the timestamp back to a specific value when reproducing a test failure
        logger.info("--> timestamp is {} (epoch: {})", timestamp, timestamp.toEpochMilli());

        final int nbBulks = randomIntBetween(1, 10);
        final int nbDocsPerBulk = randomIntBetween(1, 1000);

        for (int i = 0; i < nbBulks; i++) {
            var client = client();
            var bulkRequest = client.prepareBulk();
            for (int j = 0; j < nbDocsPerBulk; j++) {
                var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02", "vm-dev03", "vm-dev04"), "cpu-load", i);
                bulkRequest.add(client.prepareIndex(dataStreamName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
                timestamp = timestamp.plusMillis(1);
            }
            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            for (var result : bulkResponse.getItems()) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertThat(result.getVersion(), equalTo(1L));
                assertThat(result.getResponse().getPrimaryTerm(), equalTo(1L));
                var docsIdsBySeqNo = docsIdsBySeqNoAndShardId.computeIfAbsent(
                    result.getResponse().getShardId(),
                    shardId -> new HashMap<>()
                );
                var previous = docsIdsBySeqNo.put(result.getResponse().getSeqNo(), result.getId());
                assertThat(previous, nullValue());
                previous = docsIndicesById.put(result.getId(), result.getIndex());
                assertThat(previous, nullValue());
                docsIndices.add(result.getIndex());
            }
        }

        // Delete some random docs
        final List<String> deletedDocs = randomBoolean() ? randomNonEmptySubsetOf(docsIndicesById.keySet()) : List.of();
        for (var deletedDocId : deletedDocs) {
            var deletedDocIndex = docsIndicesById.get(deletedDocId);
            assertThat(deletedDocIndex, notNullValue());

            var deleteResponse = client().prepareDelete(deletedDocIndex, deletedDocId).get();
            assertThat(deleteResponse.getId(), equalTo(deletedDocId));
            assertThat(deleteResponse.getIndex(), equalTo(deletedDocIndex));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            assertThat(deleteResponse.getVersion(), equalTo(2L));
            assertThat(deleteResponse.getPrimaryTerm(), equalTo(1L));
            var docsIdsBySeqNo = docsIdsBySeqNoAndShardId.get(deleteResponse.getShardId());
            assertThat(docsIdsBySeqNo, notNullValue());
            var previous = docsIdsBySeqNo.put(deleteResponse.getSeqNo(), deletedDocId);
            assertThat(previous, nullValue());
        }

        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                if (docsIndices.contains(indexService.index().getName())) {
                    for (IndexShard indexShard : indexService) {
                        final Map<Long, String> docsIdsBySeqNo = docsIdsBySeqNoAndShardId.getOrDefault(indexShard.shardId(), Map.of());

                        // Read operations from the Translog
                        try (var translogSnapshot = getTranslog(indexShard).newSnapshot()) {
                            assertThat(translogSnapshot.totalOperations(), equalTo(docsIdsBySeqNo.size()));

                            Translog.Operation operation;
                            while ((operation = translogSnapshot.next()) != null) {
                                assertTranslogOperation(
                                    indexService.index().getName(),
                                    indexShard.mapperService().documentMapper(),
                                    operation,
                                    docsIdsBySeqNo::get,
                                    docsIndicesById::get
                                );
                            }
                        }

                        // Read operations from the Lucene index
                        try (
                            var luceneSnapshot = indexShard.newChangesSnapshot(
                                getTestName(),
                                0,
                                Long.MAX_VALUE,
                                false,
                                true,
                                true,
                                randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                            )
                        ) {
                            assertThat(luceneSnapshot.totalOperations(), equalTo(docsIdsBySeqNo.size()));

                            if (docsIdsBySeqNo.isEmpty() == false) {
                                Translog.Operation operation;
                                while ((operation = luceneSnapshot.next()) != null) {
                                    assertTranslogOperation(
                                        indexService.index().getName(),
                                        indexShard.mapperService().documentMapper(),
                                        operation,
                                        docsIdsBySeqNo::get,
                                        docsIndicesById::get
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        enum Operation {
            FLUSH,
            REFRESH,
            NONE
        }

        // Randomly executes a flush, refresh or nothing. If no flush is executed, the peer-recovery that follows will recover operations
        // from the source shard index, which load the `_id` field from stored fields (see LuceneSyntheticSourceChangesSnapshot).
        final var operation = randomFrom(Operation.values());
        switch (operation) {
            case FLUSH:
                flush(dataStreamName);
                break;
            case REFRESH:
                refresh(dataStreamName);
                break;
            case NONE:
            default:
                break;
        }

        final String[] sourceNodes = internalCluster().getNodeNames();
        final var targetNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(sourceNodes.length + 1, targetNode);

        for (var index : docsIndices) {
            updateIndexSettings(
                Settings.builder()
                    .putList(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", sourceNodes)
                    .put(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", targetNode),
                index
            );
        }

        // Wait for all shards to relocate
        final var targetNodeId = getNodeId(targetNode);
        safeAwait(
            ClusterServiceUtils.addMasterTemporaryStateListener(
                clusterState -> clusterState.projectState(ProjectId.DEFAULT)
                    .routingTable()
                    .allShards()
                    .allMatch(shardRouting -> shardRouting.started() && targetNodeId.equals(shardRouting.currentNodeId()))
            )
        );

        for (var index : docsIndices) {
            var recoveryResponse = indicesAdmin().prepareRecoveries(index).get();
            assertThat(recoveryResponse.hasRecoveries(), equalTo(true));
            for (var shardRecoveryState : recoveryResponse.shardRecoveryStates().get(index)) {
                assertThat(shardRecoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
                assertThat(shardRecoveryState.getTargetNode(), notNullValue());
                assertThat(shardRecoveryState.getTargetNode().getName(), equalTo(targetNode));
                assertThat(shardRecoveryState.getRecoverySource(), equalTo(RecoverySource.PeerRecoverySource.INSTANCE));
                assertThat(
                    shardRecoveryState.getTranslog().recoveredOperations(),
                    operation == Operation.FLUSH
                        ? equalTo(0)
                        : equalTo(docsIdsBySeqNoAndShardId.getOrDefault(shardRecoveryState.getShardId(), Map.of()).size())
                );
            }
            refresh(index);
        }

        final var nonDeletedDocs = Sets.difference(docsIndicesById.keySet(), Set.copyOf(deletedDocs));
        assertHitCount(client(targetNode).prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(0), nonDeletedDocs.size());

        var randomDocIds = randomSubsetOf(nonDeletedDocs);
        for (var docId : randomDocIds) {
            if (randomBoolean()) {
                var getResponse = client().prepareGet(docsIndicesById.get(docId), docId)
                    .setRealtime(randomBoolean())
                    .setFetchSource(randomBoolean())
                    .execute()
                    .actionGet();
                assertThat("Not found: " + docId + " " + Uid.encodeId(docId), getResponse.isExists(), equalTo(true));
                assertThat(getResponse.getVersion(), equalTo(1L));

            } else {
                assertCheckedResponse(
                    client().prepareSearch(docsIndicesById.get(docId))
                        .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, docId))),
                    searchResponse -> {
                        assertHitCount(searchResponse, 1L);
                        assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
                        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(docId));
                    }
                );
            }
        }

        randomDocIds = randomSubsetOf(deletedDocs);
        for (var docId : randomDocIds) {
            if (randomBoolean()) {
                var getResponse = client().prepareGet(docsIndicesById.get(docId), docId)
                    .setRealtime(randomBoolean())
                    .setFetchSource(randomBoolean())
                    .execute()
                    .actionGet();
                assertThat("Found deleted doc: " + docId + " " + Uid.encodeId(docId), getResponse.isExists(), equalTo(false));
                assertThat(getResponse.getVersion(), equalTo(-1L));

            } else {
                assertHitCount(
                    client().prepareSearch(docsIndicesById.get(docId))
                        .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, docId)))
                        .setSize(0),
                    0L
                );
            }
        }
    }

    public void testRecoverOperationsFromLocalTranslog() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);

        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(
            dataStreamName,
            1,
            0,
            Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.PB))
                .build()
        );

        final var docsIndices = new HashSet<String>();
        final var docsIndicesById = new HashMap<String, String>();
        final var docsIdsBySeqNo = new HashMap<Long, String>();

        var timestamp = Instant.now();
        // Use `timestamp = Instant.ofEpochMilli(epoch)` to set the timestamp back to a specific value when reproducing a test failure
        logger.info("--> timestamp is {} (epoch: {})", timestamp, timestamp.toEpochMilli());

        final int nbDocs = randomIntBetween(1, 200);

        var client = client();
        var bulkRequest = client.prepareBulk();
        for (int i = 0; i < nbDocs; i++) {
            var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02", "vm-dev03", "vm-dev04"), "cpu-load", i);
            bulkRequest.add(client.prepareIndex(dataStreamName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
            timestamp = timestamp.plusMillis(1);
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        long maxSeqNo = -1L;

        for (var result : bulkResponse.getItems()) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            assertThat(result.getResponse().getPrimaryTerm(), equalTo(1L));
            var previous = docsIdsBySeqNo.put(result.getResponse().getSeqNo(), result.getId());
            assertThat(previous, nullValue());
            previous = docsIndicesById.put(result.getId(), result.getIndex());
            assertThat(previous, nullValue());
            docsIndices.add(result.getIndex());
            maxSeqNo = Math.max(maxSeqNo, result.getResponse().getSeqNo());
        }

        // Delete some random docs
        final List<String> deletedDocs = randomBoolean() ? randomNonEmptySubsetOf(docsIndicesById.keySet()) : List.of();
        for (var deletedDocId : deletedDocs) {
            var deletedDocIndex = docsIndicesById.get(deletedDocId);
            assertThat(deletedDocIndex, notNullValue());

            var deleteResponse = client().prepareDelete(deletedDocIndex, deletedDocId).get();
            assertThat(deleteResponse.getId(), equalTo(deletedDocId));
            assertThat(deleteResponse.getIndex(), equalTo(deletedDocIndex));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            assertThat(deleteResponse.getVersion(), equalTo(2L));
            assertThat(deleteResponse.getPrimaryTerm(), equalTo(1L));
            var previous = docsIdsBySeqNo.put(deleteResponse.getSeqNo(), deletedDocId);
            assertThat(previous, nullValue());
            maxSeqNo = Math.max(maxSeqNo, deleteResponse.getSeqNo());
        }

        ensureGreen(dataStreamName);

        // Find the primary shard
        IndexShard primary = null;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                if (docsIndices.contains(indexService.index().getName())) {
                    for (IndexShard indexShard : indexService) {
                        assertThat(indexShard.routingEntry().primary(), equalTo(true));
                        primary = indexShard;
                        break;
                    }
                }
            }
        }

        assertThat(primary, notNullValue());
        IndexShard finalPrimary = primary;
        long finalMaxSeqNo = maxSeqNo;
        assertBusy(
            () -> { assertThat(finalPrimary.withEngine(engine -> engine.getLastSyncedGlobalCheckpoint()), equalTo(finalMaxSeqNo)); }
        );

        // Check translog operations on primary shard
        try (var translogSnapshot = getTranslog(primary).newSnapshot()) {
            assertThat(translogSnapshot.totalOperations(), equalTo(docsIdsBySeqNo.size()));

            Translog.Operation operation;
            while ((operation = translogSnapshot.next()) != null) {
                assertTranslogOperation(
                    primary.shardId().getIndex().getName(),
                    primary.mapperService().documentMapper(),
                    operation,
                    docsIdsBySeqNo::get,
                    docsIndicesById::get
                );
            }
        }

        // Listener to wait for the primary shard to be failed on the master node
        final var backingIndex = primary.shardId().getIndexName();
        var waitForPrimaryShardFailed = ClusterServiceUtils.addMasterTemporaryStateListener(
            state -> state.projectState(ProjectId.DEFAULT).routingTable().index(backingIndex).allPrimaryShardsUnassigned()
        );

        // Fail the primary shard
        primary.failShard("failing on purpose", new IOException("failing on purpose"));

        safeAwait(waitForPrimaryShardFailed);
        ensureGreen(primary.shardId().getIndexName());

        final long expectedRecoveredOperations = maxSeqNo + 1L;

        // Check that operations were successfully recovered locally
        var recoveryResponse = indicesAdmin().prepareRecoveries(backingIndex).get();
        assertThat(recoveryResponse.hasRecoveries(), equalTo(true));
        for (var shardRecoveryState : recoveryResponse.shardRecoveryStates().get(backingIndex)) {
            assertThat(shardRecoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
            assertThat(shardRecoveryState.getRecoverySource(), equalTo(RecoverySource.ExistingStoreRecoverySource.INSTANCE));
            assertThat((long) shardRecoveryState.getTranslog().totalOperationsOnStart(), equalTo(expectedRecoveredOperations));
            assertThat((long) shardRecoveryState.getTranslog().recoveredOperations(), equalTo(expectedRecoveredOperations));
            assertThat((long) shardRecoveryState.getTranslog().totalOperations(), equalTo(expectedRecoveredOperations));
        }

        final var nonDeletedDocs = Sets.difference(docsIndicesById.keySet(), Set.copyOf(deletedDocs));
        assertHitCount(client().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(0), nonDeletedDocs.size());

        for (var docId : randomSubsetOf(nonDeletedDocs)) {
            if (randomBoolean()) {
                var getResponse = client().prepareGet(docsIndicesById.get(docId), docId)
                    .setRealtime(randomBoolean())
                    .setFetchSource(randomBoolean())
                    .execute()
                    .actionGet();
                assertThat("Not found: " + docId + " " + Uid.encodeId(docId), getResponse.isExists(), equalTo(true));
                assertThat(getResponse.getVersion(), equalTo(1L));

            } else {
                assertCheckedResponse(
                    client().prepareSearch(docsIndicesById.get(docId))
                        .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, docId))),
                    searchResponse -> {
                        assertHitCount(searchResponse, 1L);
                        assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
                        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(docId));
                    }
                );
            }
        }

        for (var docId : randomSubsetOf(deletedDocs)) {
            if (randomBoolean()) {
                var getResponse = client().prepareGet(docsIndicesById.get(docId), docId)
                    .setRealtime(randomBoolean())
                    .setFetchSource(randomBoolean())
                    .execute()
                    .actionGet();
                assertThat("Found deleted doc: " + docId + " " + Uid.encodeId(docId), getResponse.isExists(), equalTo(false));
                assertThat(getResponse.getVersion(), equalTo(-1L));

            } else {
                assertHitCount(
                    client().prepareSearch(docsIndicesById.get(docId))
                        .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, docId)))
                        .setSize(0),
                    0L
                );
            }
        }
    }

    private static void assertTranslogOperation(
        String indexName,
        DocumentMapper documentMapper,
        Translog.Operation operation,
        Function<Long, String> expectedDocIdSupplier,
        Function<String, String> expectedDocIndexSupplier
    ) {
        final String expectedDocId;
        final BytesRef expectedDocIdEncoded;
        switch (operation.opType()) {
            case INDEX:
                final var index = asInstanceOf(Translog.Index.class, operation);
                expectedDocId = expectedDocIdSupplier.apply(index.seqNo());
                assertThat(Uid.decodeId(index.uid()), equalTo(expectedDocId));

                expectedDocIdEncoded = Uid.encodeId(expectedDocId);
                assertThat(index.uid(), equalTo(expectedDocIdEncoded));

                assertThat(expectedDocIndexSupplier.apply(expectedDocId), equalTo(indexName));
                assertThat(index.primaryTerm(), equalTo(1L));
                assertThat(index.routing(), nullValue());

                // Reproduce the parsing of the translog operations when they are replayed during recovery
                var parsedDocument = documentMapper.parse(
                    new SourceToParse(
                        Uid.decodeId(index.uid()),
                        index.source(),
                        XContentHelper.xContentType(index.source()),
                        index.routing()
                    )
                );
                assertThat(parsedDocument.id(), equalTo(expectedDocId));
                assertThat(parsedDocument.routing(), nullValue());
                assertThat(parsedDocument.docs(), hasSize(1));

                var luceneDocument = parsedDocument.docs().get(0);
                assertThat(
                    "Lucene document [" + expectedDocId + "] has wrong value for _id field",
                    luceneDocument.getField(IdFieldMapper.NAME).binaryValue(),
                    equalTo(expectedDocIdEncoded)
                );
                assertThat(
                    "Lucene document [" + expectedDocId + "] has wrong value for _tsid field",
                    luceneDocument.getField(TimeSeriesIdFieldMapper.NAME).binaryValue(),
                    equalTo(TsidExtractingIdFieldMapper.extractTimeSeriesIdFromSyntheticId(expectedDocIdEncoded))
                );
                assertThat(
                    "Lucene document [" + expectedDocId + "] has wrong value for @timestamp field",
                    luceneDocument.getField(DataStreamTimestampFieldMapper.DEFAULT_PATH).numericValue().longValue(),
                    equalTo(TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(expectedDocIdEncoded))
                );
                assertThat(
                    "Lucene document [" + expectedDocId + "] has wrong value for _ts_routing_hash field",
                    luceneDocument.getField(TimeSeriesRoutingHashFieldMapper.NAME).binaryValue(),
                    equalTo(
                        Uid.encodeId(
                            TimeSeriesRoutingHashFieldMapper.encode(
                                TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(expectedDocIdEncoded)
                            )
                        )
                    )
                );
                break;

            case DELETE:
                final var delete = asInstanceOf(Translog.Delete.class, operation);
                expectedDocId = expectedDocIdSupplier.apply(delete.seqNo());
                assertThat(Uid.decodeId(delete.uid()), equalTo(expectedDocId));

                expectedDocIdEncoded = Uid.encodeId(expectedDocId);
                assertThat(delete.uid(), equalTo(expectedDocIdEncoded));

                assertThat(expectedDocIndexSupplier.apply(expectedDocId), equalTo(indexName));
                assertThat(delete.primaryTerm(), equalTo(1L));
                break;

            default:
                throw new AssertionError("Unsupported operation type: " + operation);
        }
    }

    /**
     * Assert that we can still search by synthetic _id after restoring index from snapshot
     */
    public void testCreateSnapshot() throws IOException {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);

        // create index
        final var dataStreamName = randomIdentifier();
        int shards = randomIntBetween(1, 5);
        putDataStreamTemplate(dataStreamName, shards, 0);

        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();
        logger.info("timestamp is " + timestamp);

        var bulkItemResponses = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9)
        );

        // Verify that documents are created
        var docIdToIndex = new HashMap<String, String>();
        for (var bulkItemResponse : bulkItemResponses) {
            assertThat(bulkItemResponse.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(bulkItemResponse.getVersion(), equalTo(1L));
            docIdToIndex.put(bulkItemResponse.getId(), bulkItemResponse.getIndex());
        }

        deleteRandomDocuments(docIdToIndex).forEach(docIdToIndex::remove);

        refresh(docIdToIndex.values().toArray(String[]::new));
        Set<String> docsToVerify = docIdToIndex.isEmpty()
            ? Collections.emptySet()
            : randomSet(1, 3, () -> randomFrom(docIdToIndex.keySet()));
        Map<String, Map<String, Object>> documentSourcesBeforeSnapshot = documentSourcesAsMaps(dataStreamName, docsToVerify);

        // create snapshot
        String testRepoName = "test-repo";
        createRepository(testRepoName);
        final String snapshotName = "test-snap-" + System.currentTimeMillis();
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            testRepoName,
            snapshotName
        ).setWaitForCompletion(true).setIndices(dataStreamName).setIncludeGlobalState(false).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        // get snapshot
        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, testRepoName)
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots()
                .getFirst()
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );

        // rollover data stream
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .addIndices(dataStreamName)
            .get();
        var indexName = getIndexResponse.indices()[0];
        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(dataStreamName).get();
        assertTrue(rolloverResponse.isAcknowledged());
        assertTrue(rolloverResponse.isRolledOver());
        assertThat(
            client().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(dataStreamName).get().indices().length,
            equalTo(2)
        );

        // delete first backing index
        assertThat(client().admin().indices().prepareDelete(indexName).get().isAcknowledged(), equalTo(true));
        assertThat(
            client().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(dataStreamName).get().indices().length,
            equalTo(1)
        );
        assertThat(documentCount(dataStreamName), equalTo(0L));

        // restore from snapshot
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            testRepoName,
            snapshotName
        ).setWaitForCompletion(true).setRestoreGlobalState(false).get();
        assertNotNull(restoreSnapshotResponse.getRestoreInfo());

        // Should be able to search by (synthetic) _id
        assertSearchById(docsToVerify, docIdToIndex);

        // All documents should be there
        Map<String, Map<String, Object>> documentSourcesAfterRestore = documentSourcesAsMaps(dataStreamName, docsToVerify);
        assertThat(documentSourcesAfterRestore, equalTo(documentSourcesBeforeSnapshot));
    }

    private static long documentCount(String dataStreamName) {
        return indicesAdmin().prepareStats(dataStreamName).setDocs(true).get().getTotal().docs.getCount();
    }

    private static List<String> deleteRandomDocuments(Map<String, String> docIdToIndex) {
        List<String> deletedDocs = randomSubsetOf(randomIntBetween(1, docIdToIndex.size()), docIdToIndex.keySet());
        for (var docId : deletedDocs) {
            var deletedDocIndex = docIdToIndex.get(docId);
            assertThat(deletedDocIndex, notNullValue());

            // Delete
            var deleteResponse = client().prepareDelete(deletedDocIndex, docId).get();
            assertThat(deleteResponse.getId(), equalTo(docId));
            assertThat(deleteResponse.getIndex(), equalTo(deletedDocIndex));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            assertThat(deleteResponse.getVersion(), equalTo(2L));
        }
        return deletedDocs;
    }

    private static Map<String, Map<String, Object>> documentSourcesAsMaps(String dataStreamName, Set<String> docIds) {
        IdsQueryBuilder docIdsQuery = QueryBuilders.idsQuery().addIds(docIds.toArray(String[]::new));
        var resp = client().prepareSearch(dataStreamName).setFetchSource(true).setQuery(docIdsQuery).get();
        try {
            var result = new HashMap<String, Map<String, Object>>();
            for (SearchHit hit : resp.getHits().getHits()) {
                result.put(hit.getId(), hit.getSourceAsMap());
            }
            return result;
        } finally {
            resp.decRef();
        }
    }

    private void createRepository(String repoName) {
        Path location = randomRepoPath();
        logger.info("--> creating repository [{}] [{}]", repoName, "fs");
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", location))
        );
    }

    private static void assertSearchById(Collection<String> searchIds, HashMap<String, String> docIdToIndex) throws IOException {
        for (var docId : searchIds) {
            assertCheckedResponse(
                client().prepareSearch(docIdToIndex.get(docId))
                    .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, docId))),
                searchResponse -> {
                    assertHitCount(searchResponse, 1L);
                    assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
                    assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(docId));
                }
            );
        }
    }

    private static XContentBuilder document(Instant timestamp, String hostName, String metricField, Integer metricValue)
        throws IOException {
        var source = XContentFactory.jsonBuilder();
        source.startObject();
        {
            source.field("@timestamp", DATE_FORMATTER.format(timestamp));
            source.field("hostname", hostName);
            source.startObject("metric");
            {
                source.field("field", metricField);
                source.field("value", metricValue);

            }
            source.endObject();
        }
        source.endObject();
        return source;
    }

    private static BulkItemResponse[] createDocuments(String indexName, XContentBuilder... docs) {
        return createDocuments(indexName, true, docs);
    }

    private static BulkItemResponse[] createDocumentsWithoutValidatingTheResponse(String indexName, XContentBuilder... docs) {
        return createDocuments(indexName, false, docs);
    }

    private static BulkItemResponse[] createDocuments(String indexName, boolean validateResponse, XContentBuilder... docs) {
        assertThat(docs, notNullValue());
        final var client = client();
        var bulkRequest = client.prepareBulk();
        for (var doc : docs) {
            bulkRequest.add(client.prepareIndex(indexName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
        }
        var bulkResponse = bulkRequest.get();
        if (validateResponse) {
            assertNoFailures(bulkResponse);
        }
        return bulkResponse.getItems();
    }

    private static void putDataStreamTemplate(String indexPattern, int primaries, int replicas) throws IOException {
        putDataStreamTemplate(indexPattern, primaries, replicas, Settings.EMPTY);
    }

    private static void putDataStreamTemplate(String indexPattern, int primaries, int replicas, Settings extraSettings) throws IOException {
        final var settings = indexSettings(primaries, replicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), true);
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC);
            settings.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), randomBoolean());
        } else if (rarely()) {
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED);
        }
        settings.put(extraSettings);

        final var mappings = """
            {
                "_doc": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "hostname": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "metric": {
                            "properties": {
                                "field": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                },
                                "value": {
                                    "type": "integer",
                                    "time_series_metric": "counter"
                                }
                            }
                        }
                    }
                }
            }""";

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request(getTestClass().getName().toLowerCase(Locale.ROOT))
            .indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(indexPattern))
                    .template(new Template(settings.build(), new CompressedXContent(mappings), null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
    }

    private static IndexDiskUsageStats diskUsage(String indexName) {
        var diskUsageResponse = client().execute(
            TransportAnalyzeIndexDiskUsageAction.TYPE,
            new AnalyzeIndexDiskUsageRequest(new String[] { indexName }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, false)
        ).actionGet();

        var indexDiskUsageStats = AnalyzeIndexDiskUsageTestUtils.getIndexStats(diskUsageResponse, indexName);
        assertNotNull(indexDiskUsageStats);
        return indexDiskUsageStats;
    }
}
