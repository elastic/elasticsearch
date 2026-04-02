/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.join.ScoreMode;
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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.Lucene;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdStoredFieldsReader;
import org.elasticsearch.index.engine.Engine;
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
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.elasticsearch.index.engine.EngineTestCase.generateNewSeqNo;
import static org.elasticsearch.index.shard.IndexShardTestCase.getTranslog;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertCheckedResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
        final boolean useNestedDocs = rarely();
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, randomIntBetween(1, 5), 0, useNestedDocs);

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
            document(timestamp, "vm-dev01", "cpu-load", 0, useNestedDocs),
            document(timestamp, "vm-dev02", "cpu-load", 1, useNestedDocs),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2, useNestedDocs),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3, useNestedDocs),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4, useNestedDocs),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5, useNestedDocs),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6, useNestedDocs),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7, useNestedDocs),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8, useNestedDocs),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9, useNestedDocs)
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
                    arrayOfDocs[nbDocs] = document(t, host, "cpu-load", randomInt(10), useNestedDocs);
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

        if (useNestedDocs) {
            assertCheckedResponse(
                client().prepareSearch(dataStreamName)
                    .setTrackTotalHits(true)
                    .setQuery(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None)),
                searchResponse -> {
                    assertHitCount(searchResponse, docs.size() - deletedDocs.size());
                    for (var hit : searchResponse.getHits()) {
                        assertThat(
                            "Nested query returned deleted doc [" + hit.getId() + "]",
                            deletedDocs.contains(hit.getId()),
                            equalTo(false)
                        );
                    }
                }
            );

            for (var deletedDocId : deletedDocs) {
                var deletedDocIndex = docs.get(deletedDocId);
                assertHitCount(
                    client().prepareSearch(deletedDocIndex)
                        .setTrackTotalHits(true)
                        .setSize(0)
                        .setQuery(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(IdFieldMapper.NAME, deletedDocId))
                                .must(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None))
                        ),
                    0L
                );
            }
        }

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
            document(timestamp, "vm-dev01", "cpu-load", 0, useNestedDocs),
            document(timestamp, "vm-dev02", "cpu-load", 1, useNestedDocs),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2, useNestedDocs),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3, useNestedDocs),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4, useNestedDocs),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5, useNestedDocs),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6, useNestedDocs),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7, useNestedDocs),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8, useNestedDocs),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9, useNestedDocs)
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

        assertShardsHaveNoIdStoredFieldValuesOnDisk(indices);
    }

    public void testGetFromTranslogBySyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final boolean useNestedDocs = rarely();
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, 1, 0, useNestedDocs);

        final var docs = new HashMap<String, String>();
        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();

        // Index 5 docs in datastream
        //
        // For convenience, the metric value maps the index in the bulk response items
        var results = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0, useNestedDocs),
            document(timestamp, "vm-dev02", "cpu-load", 1, useNestedDocs),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2, useNestedDocs),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3, useNestedDocs),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4, useNestedDocs)
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
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", metricOffset, useNestedDocs),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", metricOffset + 1, useNestedDocs),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", metricOffset + 2, useNestedDocs),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", metricOffset + 3, useNestedDocs),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", metricOffset + 4, useNestedDocs)
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

        assertHitCount(client().prepareSearch(dataStreamName).setSize(0), 10L);

        if (useNestedDocs) {
            assertHitCount(
                client().prepareSearch(dataStreamName)
                    .setTrackTotalHits(true)
                    .setSize(0)
                    .setQuery(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None)),
                10L
            );
        }

        // Check that synthetic _id field have no postings on disk but has bloom filter usage
        var indices = new HashSet<>(docs.values());
        for (var index : indices) {
            var diskUsage = diskUsage(index);
            var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
            assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
            assertThat("_id field should have bloom filter usage", diskUsageIdField.getBloomFilterBytes(), greaterThan(0L));
        }

        assertShardsHaveNoIdStoredFieldValuesOnDisk(indices);
    }

    public void testRecoveredOperations() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final boolean useNestedDocs = rarely();

        // ensure a couple of nodes to have some operations coordinated
        internalCluster().ensureAtLeastNumDataNodes(2);

        final var dataStreamName = randomIdentifier();
        final int numShards = randomIntBetween(1, 10);
        putDataStreamTemplate(dataStreamName, numShards, 0, useNestedDocs);

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
                var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02", "vm-dev03", "vm-dev04"), "cpu-load", i, useNestedDocs);
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
                                    docsIndicesById::get,
                                    useNestedDocs
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
                                        docsIndicesById::get,
                                        useNestedDocs
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

        if (useNestedDocs) {
            assertHitCount(
                client(targetNode).prepareSearch(dataStreamName)
                    .setTrackTotalHits(true)
                    .setSize(0)
                    .setQuery(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None)),
                nonDeletedDocs.size()
            );
        }

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
        final boolean useNestedDocs = rarely();

        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(
            dataStreamName,
            1,
            0,
            Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.PB))
                .build(),
            useNestedDocs
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
            var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02", "vm-dev03", "vm-dev04"), "cpu-load", i, useNestedDocs);
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
                    docsIndicesById::get,
                    useNestedDocs
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

        if (useNestedDocs) {
            assertHitCount(
                client().prepareSearch(dataStreamName)
                    .setTrackTotalHits(true)
                    .setSize(0)
                    .setQuery(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None)),
                nonDeletedDocs.size()
            );
        }

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
        Function<String, String> expectedDocIndexSupplier,
        boolean useNestedDocs
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
                if (useNestedDocs) {
                    assertThat(parsedDocument.docs(), hasSize(greaterThan(1)));
                } else {
                    assertThat(parsedDocument.docs(), hasSize(1));
                }

                var luceneDocument = parsedDocument.rootDoc();
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

                for (int i = 0; i < parsedDocument.docs().size() - 1; i++) {
                    var nestedDoc = parsedDocument.docs().get(i);
                    assertThat(
                        "Nested document [" + i + "] of [" + expectedDocId + "] has wrong _id field",
                        nestedDoc.getField(IdFieldMapper.NAME).binaryValue(),
                        equalTo(expectedDocIdEncoded)
                    );
                    assertThat(
                        "Nested document [" + i + "] of [" + expectedDocId + "] has wrong _tsid field",
                        nestedDoc.getField(TimeSeriesIdFieldMapper.NAME).binaryValue(),
                        equalTo(TsidExtractingIdFieldMapper.extractTimeSeriesIdFromSyntheticId(expectedDocIdEncoded))
                    );
                    assertThat(
                        "Nested document [" + i + "] of [" + expectedDocId + "] has wrong @timestamp field",
                        nestedDoc.getField(DataStreamTimestampFieldMapper.DEFAULT_PATH).numericValue().longValue(),
                        equalTo(TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(expectedDocIdEncoded))
                    );
                    assertThat(
                        "Nested document [" + i + "] of [" + expectedDocId + "] has wrong _ts_routing_hash field",
                        nestedDoc.getField(TimeSeriesRoutingHashFieldMapper.NAME).binaryValue(),
                        equalTo(
                            Uid.encodeId(
                                TimeSeriesRoutingHashFieldMapper.encode(
                                    TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(expectedDocIdEncoded)
                                )
                            )
                        )
                    );
                }
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
        final boolean useNestedDocs = rarely();

        // create index
        final var dataStreamName = randomIdentifier();
        int shards = randomIntBetween(1, 5);
        putDataStreamTemplate(dataStreamName, shards, 0, useNestedDocs);

        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();
        logger.info("timestamp is " + timestamp);

        var bulkItemResponses = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0, useNestedDocs),
            document(timestamp, "vm-dev02", "cpu-load", 1, useNestedDocs),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2, useNestedDocs),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3, useNestedDocs),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4, useNestedDocs),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5, useNestedDocs),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6, useNestedDocs),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7, useNestedDocs),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8, useNestedDocs),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9, useNestedDocs)
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

        if (useNestedDocs) {
            assertHitCount(
                client().prepareSearch(dataStreamName)
                    .setTrackTotalHits(true)
                    .setSize(0)
                    .setQuery(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None)),
                docIdToIndex.size()
            );
        }
    }

    public void testMerge() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);

        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, 1, 0, rarely());

        final var docsIndexByIds = new ConcurrentHashMap<String, String>();
        var timestamp = Instant.now();

        final int nbBulks = randomIntBetween(12, 20);
        final int nbDocs = randomIntBetween(100, 1_000);

        for (int i = 0; i < nbBulks; i++) {
            var client = client();
            var bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < nbDocs; j++) {
                bulkRequest.add(
                    client.prepareIndex(dataStreamName).setOpType(DocWriteRequest.OpType.CREATE).setSource(String.format(Locale.ROOT, """
                        {"@timestamp": "%s", "hostname": "%s", "metric": {"field": "metric_%d", "value": %d}}
                        """, timestamp, "vm-test-" + randomIntBetween(0, 4), randomIntBetween(0, 1), randomInt()), XContentType.JSON)
                );
                timestamp = timestamp.plusMillis(10);
            }

            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);
            for (var result : bulkResponse.getItems()) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertThat(result.getVersion(), equalTo(1L));
                docsIndexByIds.put(result.getId(), result.getIndex());
            }
        }

        var indices = new HashSet<>(docsIndexByIds.values());
        for (var index : indices) {
            long segmentsCount = indicesAdmin().prepareStats(index).clear().setSegments(true).get().getPrimaries().getSegments().getCount();
            assertThat("index [" + index + "] has " + segmentsCount + " segments", segmentsCount, greaterThan(1L));
        }

        var forceMerge = indicesAdmin().prepareForceMerge(docsIndexByIds.values().toArray(String[]::new)).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));

        for (var index : indices) {
            long segmentsCount = indicesAdmin().prepareStats(index).clear().setSegments(true).get().getPrimaries().getSegments().getCount();
            assertThat("index [" + index + "] has " + segmentsCount + " segments", segmentsCount, equalTo(1L));
        }

        assertShardsHaveNoIdStoredFieldValuesOnDisk(indices);
    }

    public void testDefaultSetting() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);

        String indexName = randomIndexName();

        // Don't set IndexSettings.SYNTHETIC_ID to test default behavior.
        // Use default codec so the SYNTHETIC_ID default is true
        // (codec will be randomised by ESIntegTestCase.randomIndexTemplate if not explicitly set)
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.DEFAULT_CODEC);
        final var mapping = """
            {
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
                                "type": "keyword"
                            },
                            "value": {
                                "type": "integer",
                                "time_series_metric": "counter"
                            }
                        }
                    }
                }
            }
            """;
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(settingsBuilder).setMapping(mapping).get());

        var timestamp = Instant.now();
        createDocuments(
            indexName,
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp.plus(1, ChronoUnit.SECONDS), "vm-dev02", "cpu-load", 1)
        );
        ensureGreen(indexName);
        flushAndRefresh(indexName);

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName).get();
        String versionSetting = getSettingsResponse.getSetting(indexName, IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey());
        IndexVersion version = IndexVersion.fromId(Integer.parseInt(versionSetting));
        assertTrue(version.onOrAfter(IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_DEFAULT));
        String syntheticIdSetting = getSettingsResponse.getSetting(indexName, IndexSettings.SYNTHETIC_ID.getKey());
        assertThat(syntheticIdSetting, Matchers.nullValue());

        var diskUsage = diskUsage(indexName);
        var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
        assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
        assertThat("_id field should have bloom filter usage", diskUsageIdField.getBloomFilterBytes(), greaterThan(0L));

        var indices = new HashSet<String>();
        indices.add(indexName);
        assertShardsHaveNoIdStoredFieldValuesOnDisk(indices);
    }

    /**
     * This test verifies that index with synthetic id cannot be created
     * if index version is too low. Imagine a mixed cluster where node A has
     * support for synthetic id (post 9.4) but node B has not (pre 9.4).
     * If node A is master we don't want it to allow creation of index with
     * synthetic id until node B has been upgraded.
     */
    public void testIndexCreationIsBlockByIndexVersion() {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        String indexName = randomIndexName();
        // IndexVersion is too low for synthetic id to be allowed
        IndexVersion tooLowIndexVersion = IndexVersionUtils.randomPreviousCompatibleWriteVersion(
            IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94
        );
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.DEFAULT_CODEC)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), true)
            // In reality, we cannot set SETTING_INDEX_VERSION_CREATED explicitly (it has Property.PrivateIndex).
            // But we are lucky because the setting validation for SYNTHETIC_ID happens before SETTING_INDEX_VERSION_CREATED.
            // This is a hack but testing this in a real mixed cluster is hard because we don't have control
            // over which node is master.
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), tooLowIndexVersion)
            .build();
        final var mapping = """
            {
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
                                "type": "keyword"
                            },
                            "value": {
                                "type": "integer",
                                "time_series_metric": "counter"
                            }
                        }
                    }
                }
            }
            """;
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareCreate(indexName).setSettings(settings).setMapping(mapping).get()
        );
        assertThat(
            e.getMessage(),
            Matchers.containsString(
                String.format(
                    Locale.ROOT,
                    "The setting [%s] is only permitted for indexVersion [%d] or later. Current indexVersion: [%d].",
                    IndexSettings.SYNTHETIC_ID.getKey(),
                    IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94.id(),
                    tooLowIndexVersion.id()
                )
            )
        );
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
        return document(timestamp, hostName, metricField, metricValue, false);
    }

    private static XContentBuilder document(
        Instant timestamp,
        String hostName,
        String metricField,
        Integer metricValue,
        boolean useNestedDocs
    ) throws IOException {
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
            if (useNestedDocs) {
                int nbTags = randomIntBetween(1, 3);
                source.startArray("tags");
                for (int i = 0; i < nbTags; i++) {
                    source.startObject();
                    source.field("key", randomFrom("env", "team", "region", "service"));
                    source.field("value", randomAlphaOfLength(5));
                    source.endObject();
                }
                source.endArray();
            }
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

    private static void putDataStreamTemplate(String indexPattern, int primaries, int replicas, boolean useNestedDocs) throws IOException {
        putDataStreamTemplate(indexPattern, primaries, replicas, Settings.EMPTY, useNestedDocs);
    }

    private static void putDataStreamTemplate(
        String indexPattern,
        int primaries,
        int replicas,
        Settings extraSettings,
        boolean useNestedDocs
    ) throws IOException {
        final var settings = indexSettings(primaries, replicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), true)
            .put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), false);  // Sequence numbers are needed for id validation.
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC);
            settings.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), randomBoolean());
        } else if (rarely()) {
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED);
        }
        if (rarely()) {
            settings.put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false);
        }
        settings.put(extraSettings);

        final String nestedMapping = useNestedDocs ? """
                        ,
                        "tags": {
                            "type": "nested",
                            "properties": {
                                "key": {
                                    "type": "keyword"
                                },
                                "value": {
                                    "type": "keyword"
                                }
                            }
                        }
            """ : "";

        final var mappings = Strings.format("""
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
                        }%s
                    }
                }
            }""", nestedMapping);

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

    /**
     * Tests that no-op tombstones are correctly handled in TSDB indices with synthetic ids.
     * <p>
     * This test creates a gap in sequence numbers on the primary shard and verifies that {@link Engine#fillSeqNoGaps}
     * correctly fills the gaps with noop tombstones in three scenarios:
     * <ol>
     *   <li><b>Primary promotion</b>: After stopping the primary node, the replica is promoted to primary and fills
     *       the gaps via {@link IndexShard#updateShardState}.</li>
     *   <li><b>Snapshot restore</b>: The backing index is restored from a snapshot, and gaps are filled via
     *       {@link org.elasticsearch.index.shard.StoreRecovery#recoverFromRepository}.</li>
     *   <li><b>Peer recovery</b>: A new replica is added after restore, recovering from a primary that already has
     *       NOOP tombstones in Lucene.</li>
     * </ol>
     * The test verifies that operations (including noops) can be correctly read from the Lucene index using
     * {@link IndexShard#newChangesSnapshot}, and that GET/search by synthetic _id work correctly after each scenario.
     */
    public void testNoopTombstones() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        internalCluster().startDataOnlyNodes(2);

        final boolean useNestedDocs = rarely();
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(
            dataStreamName,
            1,
            1,  // 1 replica
            Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.PB))
                .build(),
            useNestedDocs
        );

        var timestamp = Instant.now();
        logger.info("--> timestamp is {} (epoch: {})", timestamp, timestamp.toEpochMilli());

        // Index first batch of documents
        final int nbDocsFirstBatch = randomIntBetween(1, 25);
        final var docsIdsBySeqNo = new HashMap<Long, String>();
        final var docsIndicesById = new HashMap<String, String>();

        var client = client();
        var bulkRequest = client.prepareBulk();
        for (int i = 0; i < nbDocsFirstBatch; i++) {
            var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02"), "cpu-load", i, useNestedDocs);
            bulkRequest.add(client.prepareIndex(dataStreamName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
            timestamp = timestamp.plusMillis(1);
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        String backingIndex = null;
        for (var result : bulkResponse.getItems()) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            docsIdsBySeqNo.put(result.getResponse().getSeqNo(), result.getId());
            docsIndicesById.put(result.getId(), result.getIndex());
            backingIndex = result.getIndex();
        }

        ensureGreen(dataStreamName);

        // Find the primary shard
        final var shardId = new ShardId(resolveIndex(backingIndex), 0);
        final var primaryShard = findPrimaryShard(shardId);
        assertThat(primaryShard, notNullValue());

        var clusterState = clusterService().state();
        final var primaryNodeName = clusterState.nodes().get(primaryShard.routingEntry().currentNodeId()).getName();
        var replicaRouting = clusterState.routingTable().index(shardId.getIndex()).shard(shardId.id()).replicaShards().get(0);
        final var replicaNodeName = clusterState.nodes().get(replicaRouting.currentNodeId()).getName();

        // When {@code flushBeforeSeqNoGaps} is true, documents are flushed before creating sequence number gaps, resulting in a segment
        // containing only no-op tombstones after failover. When false, documents and no-ops are mixed in the same segment(s).
        final boolean flushBeforeSeqNoGaps = randomBoolean();
        if (flushBeforeSeqNoGaps) {
            // Flush first batch to isolate documents in their own segment
            flush(backingIndex);
        }

        // Generate sequence number gaps (without indexing documents)
        final int nbGaps = randomIntBetween(1, 25);
        primaryShard.withEngine(engine -> {
            for (int i = 0; i < nbGaps; i++) {
                generateNewSeqNo(engine);
            }
            return null;
        });

        // Index second batch of documents to propagate the higher sequence numbers to the replica
        final int nbDocsSecondBatch = randomIntBetween(1, 25);
        bulkRequest = client.prepareBulk();
        for (int i = 0; i < nbDocsSecondBatch; i++) {
            var doc = document(timestamp, randomFrom("vm-dev01", "vm-dev02"), "cpu-load", nbDocsFirstBatch + i, useNestedDocs);
            bulkRequest.add(client.prepareIndex(dataStreamName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
            timestamp = timestamp.plusMillis(1);
        }
        bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        for (var result : bulkResponse.getItems()) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            docsIdsBySeqNo.put(result.getResponse().getSeqNo(), result.getId());
            docsIndicesById.put(result.getId(), result.getIndex());
        }

        final int totalDocs = nbDocsFirstBatch + nbDocsSecondBatch;

        final var repository = randomIdentifier("repo-");
        createRepository(repository);

        final var snapshot = randomIdentifier("snapshot-");
        var createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot)
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .setIndices(dataStreamName)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        if (flushBeforeSeqNoGaps) {
            // Flush second batch to isolate them in their own segment before failover
            flush(backingIndex);
        }

        var replicaShard = internalCluster().getInstance(IndicesService.class, replicaNodeName).getShardOrNull(shardId);
        assertThat(replicaShard, notNullValue());
        long expectedMaxSeqNo = totalDocs - 1 + nbGaps;
        assertThat(replicaShard.withEngine(engine -> engine.getSeqNoStats(-1).getMaxSeqNo()), equalTo(expectedMaxSeqNo));

        // Stop the primary node: this triggers failover to the replica which will fill gaps in sequence numbers with NoOp tombstone
        // operations in IndexShard.updateShardState
        internalCluster().stopNode(primaryNodeName);

        // Wait for the replica to be promoted to primary and fill the gaps
        ensureYellow(backingIndex);

        // Find the new primary (former replica)
        IndexShard newPrimary = internalCluster().getInstance(IndicesService.class, replicaNodeName).getShardOrNull(shardId);
        assertThat(newPrimary, notNullValue());
        assertThat(newPrimary.routingEntry().primary(), equalTo(true));

        // The new primary should have filled the gaps with noops
        assertBusy(() -> {
            newPrimary.withEngine(engine -> {
                assertThat(
                    "Local checkpoint should equal max seq no after filling gaps",
                    engine.getSeqNoStats(-1).getLocalCheckpoint(),
                    equalTo(engine.getSeqNoStats(-1).getMaxSeqNo())
                );
                return null;
            });
        });

        // Flush to ensure all operations (including noops) are in Lucene
        flushAndRefresh(backingIndex);

        // Check that the promoted replica has the correct number of NoOp operations
        assertNoOpTombstones(newPrimary, totalDocs, nbGaps, "primary promotion", seqNo -> docsIdsBySeqNo.get(seqNo));

        // Verify GET and search by synthetic _id after peer recovery
        assertGetAndSearchById(docsIndicesById, backingIndex, useNestedDocs);

        // Rollover the datastream
        var rolloverResponse = indicesAdmin().prepareRolloverIndex(dataStreamName).get();
        assertTrue(rolloverResponse.isShardsAcknowledged());
        assertTrue(rolloverResponse.isRolledOver());

        // Delete backing index before restore
        assertAcked(indicesAdmin().prepareDelete(backingIndex));

        // Restore the backing index from snapshot, gaps will be filled with NoOp operations in StoreRecovery.recoverFromRepository
        var restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(false)
            .setIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .get();
        assertNotNull(restoreSnapshotResponse.getRestoreInfo());

        ensureGreen(backingIndex);

        // Verify: fillSeqNoGaps was called during snapshot restore
        final var restoredShardId = new ShardId(resolveIndex(backingIndex), 0);
        final var restoredShard = findPrimaryShard(restoredShardId);
        assertNoOpTombstones(restoredShard, totalDocs, nbGaps, "after restore", seqNo -> docsIdsBySeqNo.get(seqNo));

        // Verify GET and search by synthetic _id after snapshot restore
        assertGetAndSearchById(docsIndicesById, backingIndex, useNestedDocs);

        internalCluster().startDataOnlyNode();

        // Add a replica to test peer recovery from a primary with NOOP tombstones in Lucene
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), backingIndex);
        ensureGreen(backingIndex);

        // Find the replica shard after restore
        clusterState = clusterService().state();
        replicaRouting = clusterState.routingTable().index(restoredShardId.getIndex()).shard(restoredShardId.id()).replicaShards().get(0);

        final var replicaShardAfterRestore = internalCluster().getInstance(
            IndicesService.class,
            clusterState.nodes().get(replicaRouting.currentNodeId()).getName()
        ).getShardOrNull(restoredShardId);
        assertThat(replicaShardAfterRestore, notNullValue());
        assertThat(replicaShardAfterRestore.routingEntry().primary(), equalTo(false));

        // Verify the replica has the correct number of documents and NOOP tombstones
        assertNoOpTombstones(replicaShardAfterRestore, totalDocs, nbGaps, "peer recovery with NOOPs", seqNo -> docsIdsBySeqNo.get(seqNo));

        // Verify GET and search by synthetic _id on the replica
        assertGetAndSearchById(docsIndicesById, backingIndex, useNestedDocs);
    }

    private static void assertShardsHaveNoIdStoredFieldValuesOnDisk(Set<String> indices) {
        int nbVisitedShards = 0;
        for (var indicesServices : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (var indexService : indicesServices) {
                if (indices.contains(indexService.index().getName())) {
                    for (var indexShard : indexService) {
                        long size = indexShard.withEngineOrNull(engine -> {
                            if (engine != null) {
                                try (var searcher = engine.acquireSearcher("assert_no_id_stored_field")) {
                                    long segmentsTotalSize = 0L;

                                    for (var leaf : searcher.getLeafContexts()) {
                                        var leafReader = leaf.reader();
                                        // Get the underlying stored fields reader
                                        var tsdbStoredFieldsReader = asInstanceOf(
                                            TSDBStoredFieldsFormat.TSDBStoredFieldsReader.class,
                                            Lucene.segmentReader(leafReader).getFieldsReader()
                                        );

                                        // Extract the real (ie, non-synthetic id) stored field reader
                                        final var defaultStoredFields = tsdbStoredFieldsReader.getStoredFieldsReader();
                                        assertThat(defaultStoredFields, not(instanceOf(TSDBSyntheticIdStoredFieldsReader.class)));

                                        final var fieldInfo = leafReader.getFieldInfos().fieldInfo(IdFieldMapper.NAME);
                                        assertThat(fieldInfo, notNullValue());

                                        // Visit the "_id" field and compute its total size accross all documents
                                        final var visitor = new StoredFieldVisitor() {
                                            long segmentSize = 0L;
                                            int visitedDocs = -1;

                                            @Override
                                            public Status needsField(FieldInfo fieldInfo) throws IOException {
                                                return IdFieldMapper.NAME.equals(fieldInfo.getName())
                                                    ? StoredFieldVisitor.Status.YES
                                                    : Status.NO;
                                            }

                                            @Override
                                            public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                                                segmentSize += value.length;
                                                if (visitedDocs == -1) {
                                                    visitedDocs = 1;
                                                } else {
                                                    visitedDocs++;
                                                }
                                            }
                                        };

                                        for (int docID = 0; docID < leafReader.maxDoc(); docID++) {
                                            defaultStoredFields.document(docID, visitor);
                                        }
                                        assertThat(visitor.visitedDocs, anyOf(equalTo(leafReader.maxDoc() - 1), equalTo(-1)));
                                        segmentsTotalSize += visitor.segmentSize;
                                    }
                                    return segmentsTotalSize;
                                } catch (IOException ioe) {
                                    throw new AssertionError(ioe);
                                }
                            }
                            return 0L;
                        });

                        assertThat(
                            "Found non-zero total size for [_id] stored field values on shard " + indexShard.routingEntry(),
                            size,
                            equalTo(0L)
                        );
                        nbVisitedShards++;
                    }
                }
            }
        }
        assertThat("Expect at least 1 shard per index to be verified", nbVisitedShards, greaterThanOrEqualTo(indices.size()));
    }

    private static IndexShard findPrimaryShard(ShardId shardId) {
        for (String node : internalCluster().getNodeNames()) {
            var indicesService = internalCluster().getInstance(IndicesService.class, node);
            var indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                IndexShard shard = indexService.getShardOrNull(shardId.getId());
                if (shard != null && shard.isActive() && shard.routingEntry().primary()) {
                    return shard;
                }
            }
        }
        throw new AssertionError("IndexShard instance not found for shard " + shardId);
    }

    private static void assertNoOpTombstones(
        IndexShard indexShard,
        int totalDocs,
        int nbGaps,
        String reason,
        Function<Long, String> seqNoToDocId
    ) throws IOException {
        // Read operations from Lucene using newChangesSnapshot
        try (
            var luceneSnapshot = indexShard.newChangesSnapshot(
                reason,
                0,
                Long.MAX_VALUE,
                false,
                true,
                true,
                randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
            )
        ) {
            assertThat(luceneSnapshot.totalOperations(), equalTo(totalDocs + nbGaps));

            Translog.Operation operation;
            int indexOps = 0;
            int noopOps = 0;
            while ((operation = luceneSnapshot.next()) != null) {
                switch (operation.opType()) {
                    case INDEX:
                        final var index = asInstanceOf(Translog.Index.class, operation);
                        String expectedDocId = seqNoToDocId.apply(index.seqNo());
                        assertThat(
                            "Index operation seqNo=" + index.seqNo() + " should have expected id",
                            Uid.decodeId(index.uid()),
                            equalTo(expectedDocId)
                        );
                        indexOps++;
                        break;
                    case NO_OP:
                        noopOps++;
                        break;
                    default:
                        fail("Unexpected operation type: " + operation.opType());
                }
            }
            assertThat("Should have read all index operations", indexOps, equalTo(totalDocs));
            assertThat("Should have read all noop operations (filled gaps)", noopOps, equalTo(nbGaps));
        }
    }

    private void assertGetAndSearchById(Map<String, String> docsIndicesById, String backingIndex, boolean useNestedDocs)
        throws IOException {
        refresh(backingIndex);

        // Verify total hit count
        assertHitCount(client().prepareSearch(backingIndex).setTrackTotalHits(true).setSize(0), docsIndicesById.size());

        // Random GET/search by synthetic _id
        var randomDocIds = randomSubsetOf(docsIndicesById.keySet());
        for (var docId : randomDocIds) {
            if (randomBoolean()) {
                // GET by synthetic _id
                var getResponse = client().prepareGet(docsIndicesById.get(docId), docId)
                    .setRealtime(randomBoolean())
                    .setFetchSource(randomBoolean())
                    .execute()
                    .actionGet();
                assertThat(getResponse.isExists(), equalTo(true));
                assertThat(getResponse.getVersion(), equalTo(1L));
            } else {
                // Search by synthetic _id
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

        // Nested query verification
        if (useNestedDocs) {
            assertHitCount(
                client().prepareSearch(backingIndex)
                    .setTrackTotalHits(true)
                    .setSize(0)
                    .setQuery(QueryBuilders.nestedQuery("tags", QueryBuilders.existsQuery("tags.key"), ScoreMode.None)),
                docsIndicesById.size()
            );
        }
    }
}
