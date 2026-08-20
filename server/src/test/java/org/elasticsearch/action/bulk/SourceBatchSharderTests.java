/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SourceBatchSharderTests extends ESTestCase {

    private static final String DATA_STREAM = "metrics-app-default";
    /** Fixed epoch for backing index names so tests do not depend on wall-clock time. */
    private static final long EPOCH_MILLIS = 1704067200000L; // 2024-01-01T00:00:00Z

    private static final String GEN_1_START = "2024-01-01T00:00:00Z";
    private static final String GEN_1_END = "2024-06-01T00:00:00Z";
    private static final String GEN_2_START = "2024-06-01T00:00:00Z";
    private static final String GEN_2_END = "2025-01-01T00:00:00Z";

    private static final Instant IN_GEN_1 = Instant.parse("2024-03-01T00:00:00Z");
    private static final Instant IN_GEN_2 = Instant.parse("2024-09-01T00:00:00Z");

    /** Builds a plain {@link IndexMetadata} with no routing path (Unpartitioned strategy). */
    private static IndexMetadata plainMetadata(String name, int shards) {
        return IndexMetadata.builder(name).settings(indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)).build();
    }

    /**
     * Builds a TSDB backing index whose routing strategy is {@link IndexRouting.ExtractFromSource.ForIndexDimensions}:
     * time_series mode plus a non-empty {@code index.dimensions}, which is what selects that strategy in
     * {@link IndexRouting#fromIndexMetadata}.
     */
    private static IndexMetadata tsdbBackingIndex(int generation, int shards, String start, String end) {
        String name = DataStream.getDefaultBackingIndexName(DATA_STREAM, generation, EPOCH_MILLIS);
        return IndexMetadata.builder(name)
            .settings(
                indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                    .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), start)
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), end)
            )
            .build();
    }

    /**
     * Builds a TSDB backing index that routes via {@code index.routing_path}, i.e.
     * {@link IndexRouting.ExtractFromSource.ForRoutingPath} — a strategy the sharder must reject because it
     * can only be evaluated by parsing _source.
     */
    private static IndexMetadata routingPathBackingIndex(int generation, int shards, String start, String end) {
        String name = DataStream.getDefaultBackingIndexName(DATA_STREAM, generation, EPOCH_MILLIS);
        return IndexMetadata.builder(name)
            .settings(
                indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), start)
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), end)
            )
            .build();
    }

    private static Settings.Builder indexSettings(String indexName) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, indexName + "-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
    }

    private static ProjectMetadata project(IndexMetadata... indices) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        for (IndexMetadata index : indices) {
            builder.put(index, false);
        }
        return builder.build();
    }

    /** A time series data stream over the given backing indices, in generation order. */
    private static ProjectMetadata projectWithDataStream(IndexMetadata... backingIndices) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        List<Index> indices = new ArrayList<>();
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
            indices.add(index.getIndex());
        }
        builder.put(
            DataStream.builder(DATA_STREAM, indices).setGeneration(backingIndices.length).setIndexMode(IndexMode.TIME_SERIES).build()
        );
        return builder.build();
    }

    /** The documents backing a batch, kept so tests can assert the rows survived the scatter unchanged. */
    private record Docs(EscfBatch batch, List<BytesReference> sources) {}

    /** Builds a batch of {@code n} rows, each {@code {"dim": "d<i>", "val": i}}. */
    private static Docs buildDocs(int n) throws IOException {
        List<BytesReference> sources = new ArrayList<>(n);
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (int i = 0; i < n; i++) {
                XContentBuilder doc = JsonXContent.contentBuilder();
                doc.startObject();
                doc.field("dim", "d" + i);
                doc.field("val", (long) i);
                doc.endObject();
                BytesReference source = BytesReference.bytes(doc);
                sources.add(source);
                encoder.parseToScratch(source, XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            return new Docs(encoder.buildPartition(0), sources);
        }
    }

    private static EscfBatch buildBatch(int n) throws IOException {
        return buildDocs(n).batch();
    }

    /**
     * Builds sourceless {@link IndexRequest}s referencing rows {@code 0..numDocs-1} of {@code batch} and
     * attaches the batch under {@code batchKey} — the name the requests target, which is what the sharder
     * keys on.
     */
    private static BulkRequest buildBulkRequest(String batchKey, EscfBatch batch, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(rowRequest(batchKey, batch, i));
        }
        bulkRequest.setPreBuiltBatches(Map.of(batchKey, batch));
        return bulkRequest;
    }

    private static IndexRequest rowRequest(String indexName, EscfBatch batch, int row) {
        IndexRequest request = new IndexRequest(indexName).id("doc-" + row).opType(DocWriteRequest.OpType.INDEX);
        request.indexSource().setSourceRow(batch, row, XContentType.JSON);
        return request;
    }

    /** A row-bearing request for a TSDB data stream: create-only, with the timestamp and tsid pre-computed. */
    private static IndexRequest tsdbRowRequest(String indexName, EscfBatch batch, int row, Instant timestamp) {
        IndexRequest request = new IndexRequest(indexName).opType(DocWriteRequest.OpType.CREATE);
        request.indexSource().setSourceRow(batch, row, XContentType.JSON);
        // The source is empty, so both of these must be supplied by the batch producer.
        request.setTimeSeriesTimestamp(timestamp);
        request.tsid(new BytesRef("tsid-" + row));
        return request;
    }

    /**
     * Mirror of {@link BulkOperation}'s shard grouping loop for the parts the sharder participates in:
     * resolve the concrete write index of each item, then preRouting → prepareRouting → route →
     * postRouting → recordRouting, accumulating items per shard in bulk order.
     *
     * @param skipRows rows to drop before routing, standing in for items that fail validation in the real loop
     */
    private static Map<ShardId, List<BulkItemRequest>> routeAll(
        SourceBatchSharder sharder,
        BulkRequest bulkRequest,
        ProjectMetadata project,
        Set<Integer> skipRows
    ) {
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new LinkedHashMap<>();
        int slot = 0;
        for (DocWriteRequest<?> docWriteRequest : bulkRequest.requests) {
            IndexRequest request = (IndexRequest) docWriteRequest;
            BulkItemRequest item = new BulkItemRequest(slot++, request);
            if (skipRows.contains(request.indexSource().rowIndex())) {
                continue;
            }
            IndexAbstraction abstraction = project.getIndicesLookup().get(request.index());
            Index concreteIndex = request.getConcreteWriteIndex(abstraction, project);
            IndexRouting routing = IndexRouting.fromIndexMetadata(project.getIndexSafe(concreteIndex));
            request.preRoutingProcess(routing);
            SourceBatchSharder.ConcreteIndexTarget target = sharder == null
                ? null
                : sharder.prepareRouting(request, concreteIndex, routing, project);
            int shardId = request.route(routing);
            request.postRoutingProcess(routing);
            if (target != null) {
                target.recordRoutedShard(request, shardId);
            }
            requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), ignored -> new ArrayList<>()).add(item);
        }
        return requestsByShard;
    }

    private static Map<ShardId, List<BulkItemRequest>> routeAll(
        SourceBatchSharder sharder,
        BulkRequest bulkRequest,
        ProjectMetadata project
    ) {
        return routeAll(sharder, bulkRequest, project, Set.of());
    }

    /** Asserts every shard's items map 1:1 and in order onto its batch's rows. */
    private static void assertShardsAligned(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        SourceBatchSharder.validateBatchAlignment(requestsByShard, shardBatches);
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            SourceBatch shardBatch = shardBatches.get(entry.getKey());
            assertThat("no batch for shard " + entry.getKey(), shardBatch, notNullValue());
            assertThat("row count for shard " + entry.getKey(), shardBatch.docCount(), equalTo(entry.getValue().size()));
            assertTrue("rows not aligned for shard " + entry.getKey(), BulkShardBatch.rowsAlignWithItems(shardBatch, entry.getValue()));
        }
    }

    private static Map<String, Object> asMap(BytesReference source) {
        return XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
    }

    public void testCreateReturnsNullWhenNoBatches() {
        assertThat(SourceBatchSharder.create(new BulkRequest()), nullValue());
    }

    public void testCreateReturnsNullWhenEmptyBatchMap() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of());
        assertThat(SourceBatchSharder.create(request), nullValue());
    }

    /**
     * A {@link SourceBatch} that is not an {@link EscfBatch} is unreachable through any production code path
     * today, so a stub is the only way to exercise the guard. It throws from every method to make it obvious
     * if anything other than the {@code instanceof} check ever touches it.
     */
    private static class NotAnEscfBatch implements SourceBatch {
        @Override
        public int docCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceSchema schema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int columnCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesReference data() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceRow row(int docIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceBatch slice(int from, int to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    public void testCreateThrowsForNonEscfBatch() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("myindex", new NotAnEscfBatch()));
        var e = expectThrows(IllegalArgumentException.class, () -> SourceBatchSharder.create(request));
        assertThat(e.getMessage(), containsString("must be an EscfBatch"));
    }

    // -------------------------------------------------------------------------
    // Tests: single concrete index
    // -------------------------------------------------------------------------

    public void testSingleShardAllRowsRouted() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        assertThat(sharder, notNullValue());
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertThat(result.size(), equalTo(1));
        SourceBatch shardBatch = result.get(new ShardId(md.getIndex(), 0));
        assertThat(shardBatch, notNullValue());
        assertThat(shardBatch.docCount(), equalTo(numDocs));
        assertShardsAligned(requestsByShard, result);
        sharder.close();
    }

    public void testMultiShardRowsAlignWithItems() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        int numShards = randomIntBetween(2, 5);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", numShards);
        ProjectMetadata project = project(md);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertShardsAligned(requestsByShard, result);
        assertThat(result.size(), equalTo(requestsByShard.size()));
        sharder.close();
    }

    /**
     * The case the sharder exists to handle: one producer batch keyed by the data stream name, whose rows a
     * TSDB data stream splits per document across two backing indices with different shard counts. Rows
     * alternate between generations so the two indices interleave in the batch.
     */
    public void testFanOutAcrossBackingIndicesWithDifferentShardCounts() throws IOException {
        int numDocs = randomIntBetween(10, 40);
        IndexMetadata gen1 = tsdbBackingIndex(1, 3, GEN_1_START, GEN_1_END);
        IndexMetadata gen2 = tsdbBackingIndex(2, 5, GEN_2_START, GEN_2_END);
        ProjectMetadata project = projectWithDataStream(gen1, gen2);
        assertThat(IndexRouting.fromIndexMetadata(gen1), instanceOf(IndexRouting.ExtractFromSource.ForIndexDimensions.class));

        Docs docs = buildDocs(numDocs);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(tsdbRowRequest(DATA_STREAM, docs.batch(), i, i % 2 == 0 ? IN_GEN_1 : IN_GEN_2));
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, docs.batch()));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        // Both backing indices received rows, and every shard's rows line up with its items.
        Set<Index> indices = new HashSet<>();
        int totalRows = 0;
        for (Map.Entry<ShardId, SourceBatch> entry : result.entrySet()) {
            indices.add(entry.getKey().getIndex());
            totalRows += entry.getValue().docCount();
        }
        assertThat(indices, equalTo(Set.of(gen1.getIndex(), gen2.getIndex())));
        assertThat(totalRows, equalTo(numDocs));
        assertShardsAligned(requestsByShard, result);

        // Strongest statement available: every item still materializes the document it was built from.
        for (int i = 0; i < numDocs; i++) {
            IndexRequest request = (IndexRequest) bulkRequest.requests.get(i);
            request.indexSource().ensureInlineSource();
            assertThat("row " + i + " content", asMap(request.indexSource().bytes()), equalTo(asMap(docs.sources().get(i))));
        }
        sharder.close();
    }

    /** Each (concrete index, shard) pair must get its own partition: same shard number, different indices. */
    public void testFanOutPartitionsAreDisjoint() throws IOException {
        int numDocs = 40;
        IndexMetadata gen1 = tsdbBackingIndex(1, 3, GEN_1_START, GEN_1_END);
        IndexMetadata gen2 = tsdbBackingIndex(2, 3, GEN_2_START, GEN_2_END);
        ProjectMetadata project = projectWithDataStream(gen1, gen2);

        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, i, i % 2 == 0 ? IN_GEN_1 : IN_GEN_2));
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertShardsAligned(requestsByShard, result);
        // No batch instance may be shared between two shards, in particular not between shard k of gen1 and
        // shard k of gen2.
        Map<SourceBatch, ShardId> seen = new IdentityHashMap<>();
        for (Map.Entry<ShardId, SourceBatch> entry : result.entrySet()) {
            ShardId previous = seen.put(entry.getValue(), entry.getKey());
            assertThat("batch shared by " + previous + " and " + entry.getKey(), previous, nullValue());
        }
        assertThat(result.size(), greaterThan(1));
        sharder.close();
    }

    /**
     * Backing indices are validated independently: a generation that still routes via {@code routing_path}
     * cannot be used with a pre-built batch, while a newer generation on {@code _tsid} routing can.
     */
    public void testRoutingStrategyIsValidatedPerBackingIndex() throws IOException {
        IndexMetadata gen1 = routingPathBackingIndex(1, 2, GEN_1_START, GEN_1_END);
        IndexMetadata gen2 = tsdbBackingIndex(2, 2, GEN_2_START, GEN_2_END);
        ProjectMetadata project = projectWithDataStream(gen1, gen2);
        assertThat(IndexRouting.fromIndexMetadata(gen1), instanceOf(IndexRouting.ExtractFromSource.ForRoutingPath.class));

        EscfBatch batch = buildBatch(4);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 4; i++) {
            bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, i, i % 2 == 0 ? IN_GEN_1 : IN_GEN_2));
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));
        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);

        // Rows 0 and 2 go to the routing_path generation and must be rejected; rows 1 and 3 route fine.
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new LinkedHashMap<>();
        for (int i = 0; i < 4; i++) {
            IndexRequest request = (IndexRequest) bulkRequest.requests.get(i);
            IndexAbstraction abstraction = project.getIndicesLookup().get(DATA_STREAM);
            Index concreteIndex = request.getConcreteWriteIndex(abstraction, project);
            IndexRouting routing = IndexRouting.fromIndexMetadata(project.getIndexSafe(concreteIndex));
            request.preRoutingProcess(routing);
            if (concreteIndex.equals(gen1.getIndex())) {
                var e = expectThrows(
                    IllegalArgumentException.class,
                    () -> sharder.prepareRouting(request, concreteIndex, routing, project)
                );
                assertThat(e.getMessage(), containsString("routes by extracting fields from _source"));
                continue;
            }
            SourceBatchSharder.ConcreteIndexTarget target = sharder.prepareRouting(request, concreteIndex, routing, project);
            int shardId = request.route(routing);
            request.postRoutingProcess(routing);
            target.recordRoutedShard(request, shardId);
            requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), ignored -> new ArrayList<>())
                .add(new BulkItemRequest(i, request));
        }

        Map<ShardId, SourceBatch> result = sharder.shardBatches();
        assertShardsAligned(requestsByShard, result);
        // The rejected rows went to the discard bucket, so only the two accepted rows are present.
        int totalRows = result.values().stream().mapToInt(SourceBatch::docCount).sum();
        assertThat(totalRows, equalTo(2));
        sharder.close();
    }

    public void testDroppedRowsGoToDiscardBucket() throws IOException {
        int numDocs = randomIntBetween(6, 20);
        int numShards = randomIntBetween(1, 4);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", numShards));

        Set<Integer> dropped = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                dropped.add(i);
            }
        }
        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project, dropped);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        int totalRows = result.values().stream().mapToInt(SourceBatch::docCount).sum();
        assertThat(totalRows, equalTo(numDocs - dropped.size()));
        assertShardsAligned(requestsByShard, result);
        sharder.close();
    }

    public void testAllRowsDroppedProducesNoBatches() throws IOException {
        EscfBatch batch = buildBatch(5);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, 5);
        ProjectMetadata project = project(plainMetadata("myindex", 2));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project, Set.of(0, 1, 2, 3, 4));
        assertTrue(requestsByShard.isEmpty());
        assertTrue(sharder.shardBatches().isEmpty());
        sharder.close();
    }

    /**
     * The failure-store redirect pass re-enters {@code executeBulkRequestsByShard}, so {@code shardBatches()}
     * can be called a second time. It must not re-scatter: the first call's batches are already attached to
     * in-flight shard requests and their items already point at shard-local rows.
     */
    public void testSecondShardBatchesCallIsANoOp() throws IOException {
        int numDocs = 12;
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", 3));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> first = sharder.shardBatches();
        assertFalse(first.isEmpty());

        List<Integer> rowsAfterFirst = bulkRequest.requests.stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList();
        assertThat(sharder.shardBatches(), equalTo(Map.of()));
        List<Integer> rowsAfterSecond = bulkRequest.requests.stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList();
        assertThat(rowsAfterSecond, equalTo(rowsAfterFirst));
        assertShardsAligned(requestsByShard, first);
        sharder.close();
    }

    public void testRejectsItemWithoutSourceRow() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);
        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);

        // Item has inline source — no source-row reference.
        IndexRequest request = new IndexRequest("myindex").id("doc-0").source(new HashMap<>());
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> sharder.prepareRouting(request, md.getIndex(), IndexRouting.fromIndexMetadata(md), project)
        );
        assertThat(e.getMessage(), containsString("must carry a source-row reference"));
        sharder.close();
    }

    public void testRejectsRowBearingItemWithNoBatchForItsName() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        IndexMetadata other = plainMetadata("otherindex", 1);
        ProjectMetadata project = project(plainMetadata("myindex", 1), other);
        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);

        // Carries a row but targets a name with no batch — e.g. because something rewrote _index.
        IndexRequest request = rowRequest("otherindex", batch, 0);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> sharder.prepareRouting(request, other.getIndex(), IndexRouting.fromIndexMetadata(other), project)
        );
        assertThat(e.getMessage(), containsString("no pre-built batch was supplied under that name"));
        sharder.close();
    }

    public void testRejectsInlineItemForAnUnbatchedName() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        IndexMetadata other = plainMetadata("otherindex", 1);
        ProjectMetadata project = project(plainMetadata("myindex", 1), other);
        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);

        // Inline source in a bulk that carries batches: its shard's rows could not line up with its items.
        IndexRequest request = new IndexRequest("otherindex").id("doc-0").source(new HashMap<>());
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> sharder.prepareRouting(request, other.getIndex(), IndexRouting.fromIndexMetadata(other), project)
        );
        assertThat(e.getMessage(), containsString("the two cannot be mixed"));
        sharder.close();
    }

    public void testRejectsNonIndexRequestItem() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> SourceBatchSharder.requireBatchItem(new DeleteRequest("myindex", "doc-0"))
        );
        assertThat(e.getMessage(), containsString("cannot be mixed with pre-built source batches"));
    }

    public void testRequireBatchItemPassesThroughIndexRequests() throws IOException {
        EscfBatch batch = buildBatch(1);
        IndexRequest request = rowRequest("myindex", batch, 0);
        assertThat(SourceBatchSharder.requireBatchItem(request), sameInstance(request));
    }

    public void testRejectsTwoBatchesFeedingOneConcreteIndex() throws IOException {
        EscfBatch viaName = buildBatch(2);
        EscfBatch viaAlias = buildBatch(2);
        IndexMetadata md = IndexMetadata.builder("myindex")
            .settings(indexSettings("myindex").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .putAlias(AliasMetadata.builder("myalias").writeIndex(true).build())
            .build();
        ProjectMetadata project = project(md);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(rowRequest("myindex", viaName, 0));
        bulkRequest.add(rowRequest("myalias", viaAlias, 0));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", viaName, "myalias", viaAlias));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        SourceBatchSharder.ConcreteIndexTarget target = sharder.prepareRouting(first, md.getIndex(), routing, project);
        target.recordRoutedShard(first, first.route(routing));

        IndexRequest second = (IndexRequest) bulkRequest.requests.get(1);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.prepareRouting(second, md.getIndex(), routing, project));
        assertThat(e.getMessage(), containsString("may only be fed by one batch"));

        // The first batch is unaffected and still shards correctly.
        Map<ShardId, SourceBatch> result = sharder.shardBatches();
        assertThat(result.get(new ShardId(md.getIndex(), 0)).docCount(), equalTo(1));
        sharder.close();
    }

    public void testRejectsNonMonotonicRowIndex() throws IOException {
        EscfBatch batch = buildBatch(3);
        BulkRequest bulkRequest = new BulkRequest();
        // Items in reverse row order — should fail on the second item.
        for (int i = 2; i >= 0; i--) {
            bulkRequest.add(rowRequest("myindex", batch, i));
        }
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);

        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        sharder.prepareRouting(first, md.getIndex(), routing, project).recordRoutedShard(first, 0);
        IndexRequest second = (IndexRequest) bulkRequest.requests.get(1);
        SourceBatchSharder.ConcreteIndexTarget target = sharder.prepareRouting(second, md.getIndex(), routing, project);
        var e = expectThrows(IllegalArgumentException.class, () -> target.recordRoutedShard(second, 0));
        assertThat(e.getMessage(), containsString("not strictly greater"));
        sharder.close();
    }

    public void testRejectsShardIdOutsideShardCount() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, 1);
        IndexMetadata md = plainMetadata("myindex", 2);
        ProjectMetadata project = project(md);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        IndexRequest request = (IndexRequest) bulkRequest.requests.get(0);
        SourceBatchSharder.ConcreteIndexTarget target = sharder.prepareRouting(
            request,
            md.getIndex(),
            IndexRouting.fromIndexMetadata(md),
            project
        );
        var e = expectThrows(IllegalStateException.class, () -> target.recordRoutedShard(request, 2));
        assertThat(e.getMessage(), containsString("outside the shard count"));
        sharder.close();
    }

    public void testRejectsForIndexDimensionsWithoutTsid() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        assertThat(routing, instanceOf(IndexRouting.ExtractFromSource.ForIndexDimensions.class));
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(1);
        IndexRequest request = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        request.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        request.setTimeSeriesTimestamp(IN_GEN_1); // but no tsid
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.prepareRouting(request, md.getIndex(), routing, project));
        assertThat(e.getMessage(), containsString("routes on _tsid"));
        sharder.close();
    }

    public void testForIndexDimensionsWithTsidSucceeds() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1));
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        assertShardsAligned(requestsByShard, sharder.shardBatches());
        sharder.close();
    }

    public void testSingleShardPassthroughHandsSourceBatchThrough() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        Docs docs = buildDocs(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", docs.batch(), numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertThat(result.size(), equalTo(1));
        assertSame(docs.batch(), result.get(new ShardId(md.getIndex(), 0)));
        assertShardsAligned(requestsByShard, result);

        // The items were never re-pointed, so each one still materializes the document it was built from.
        for (int i = 0; i < numDocs; i++) {
            IndexRequest request = (IndexRequest) bulkRequest.requests.get(i);
            assertThat(request.indexSource().rowIndex(), equalTo(i));
            request.indexSource().ensureInlineSource();
            assertThat("row " + i + " content", asMap(request.indexSource().bytes()), equalTo(asMap(docs.sources().get(i))));
        }
        sharder.close();
    }

    /** A dropped row has to be filtered out of the batch, so a single shard is not enough to pass through. */
    public void testSingleShardWithDroppedRowsStillScatters() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project, Set.of(randomIntBetween(0, numDocs - 1)));
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        SourceBatch shardBatch = result.get(new ShardId(md.getIndex(), 0));
        assertNotSame(batch, shardBatch);
        assertThat(shardBatch.docCount(), equalTo(numDocs - 1));
        assertShardsAligned(requestsByShard, result);
        sharder.close();
    }

    /** More than one shard means the rows genuinely have to be split, whatever they happened to route to. */
    public void testMultiShardDoesNotPassThrough() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", randomIntBetween(2, 5)));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        for (Map.Entry<ShardId, SourceBatch> entry : result.entrySet()) {
            assertNotSame("shard " + entry.getKey() + " was handed the whole batch", batch, entry.getValue());
        }
        assertShardsAligned(requestsByShard, result);
        sharder.close();
    }

    /** The passthrough is decided per batch, not per bulk: two single-shard batches both take it. */
    public void testEachSingleShardGroupPassesThrough() throws IOException {
        int rowsPerIndex = randomIntBetween(2, 10);
        EscfBatch batchA = buildBatch(rowsPerIndex);
        EscfBatch batchB = buildBatch(rowsPerIndex);
        IndexMetadata mdA = plainMetadata("index-a", 1);
        IndexMetadata mdB = plainMetadata("index-b", 1);
        ProjectMetadata project = project(mdA, mdB);

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < rowsPerIndex; i++) {
            bulkRequest.add(rowRequest("index-a", batchA, i));
            bulkRequest.add(rowRequest("index-b", batchB, i));
        }
        bulkRequest.setPreBuiltBatches(Map.of("index-a", batchA, "index-b", batchB));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertThat(result.size(), equalTo(2));
        assertSame(batchA, result.get(new ShardId(mdA.getIndex(), 0)));
        assertSame(batchB, result.get(new ShardId(mdB.getIndex(), 0)));
        assertShardsAligned(requestsByShard, result);
        sharder.close();
    }

    public void testFanOutOfSingleShardBackingIndicesStillScatters() throws IOException {
        int numDocs = randomIntBetween(4, 20);
        IndexMetadata gen1 = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        IndexMetadata gen2 = tsdbBackingIndex(2, 1, GEN_2_START, GEN_2_END);
        ProjectMetadata project = projectWithDataStream(gen1, gen2);

        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, i, i % 2 == 0 ? IN_GEN_1 : IN_GEN_2));
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertThat(result.keySet(), equalTo(Set.of(new ShardId(gen1.getIndex(), 0), new ShardId(gen2.getIndex(), 0))));
        for (Map.Entry<ShardId, SourceBatch> entry : result.entrySet()) {
            assertNotSame("shard " + entry.getKey() + " was handed the whole batch", batch, entry.getValue());
        }
        assertThat(result.values().stream().mapToInt(SourceBatch::docCount).sum(), equalTo(numDocs));
        assertShardsAligned(requestsByShard, result);
        sharder.close();
    }

    public void testInterleavedBatchNamesResolveCorrectly() throws IOException {
        int rowsPerIndex = randomIntBetween(4, 15);
        Docs docsA = buildDocs(rowsPerIndex);
        Docs docsB = buildDocs(rowsPerIndex);
        ProjectMetadata project = project(plainMetadata("index-a", 3), plainMetadata("index-b", 2));

        BulkRequest bulkRequest = new BulkRequest();
        List<IndexRequest> aRequests = new ArrayList<>();
        List<IndexRequest> bRequests = new ArrayList<>();
        for (int i = 0; i < rowsPerIndex; i++) {
            IndexRequest a = rowRequest("index-a", docsA.batch(), i);
            IndexRequest b = rowRequest("index-b", docsB.batch(), i);
            aRequests.add(a);
            bRequests.add(b);
            bulkRequest.add(a);
            bulkRequest.add(b);
        }
        bulkRequest.setPreBuiltBatches(Map.of("index-a", docsA.batch(), "index-b", docsB.batch()));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        var requestsByShard = routeAll(sharder, bulkRequest, project);
        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        assertShardsAligned(requestsByShard, result);
        // Each item still resolves to the document its own batch held for that row.
        for (int i = 0; i < rowsPerIndex; i++) {
            aRequests.get(i).indexSource().ensureInlineSource();
            assertThat("index-a row " + i, asMap(aRequests.get(i).indexSource().bytes()), equalTo(asMap(docsA.sources().get(i))));
            bRequests.get(i).indexSource().ensureInlineSource();
            assertThat("index-b row " + i, asMap(bRequests.get(i).indexSource().bytes()), equalTo(asMap(docsB.sources().get(i))));
        }
        sharder.close();
    }

    /** Resolving an already-bound target must not skip the per-item {@code _tsid} check. */
    public void testBoundTargetStillValidatesTsid() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(2);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1));
        IndexRequest withoutTsid = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        withoutTsid.indexSource().setSourceRow(batch, 1, XContentType.JSON);
        withoutTsid.setTimeSeriesTimestamp(IN_GEN_1); // but no tsid
        bulkRequest.add(withoutTsid);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        // The first item binds the target; the second resolves the same one and must still be rejected.
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        sharder.prepareRouting(first, md.getIndex(), routing, project);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.prepareRouting(withoutTsid, md.getIndex(), routing, project));
        assertThat(e.getMessage(), containsString("routes on _tsid"));
        sharder.close();
    }

    /** A bulk with a single batch still checks the name every item targets, not just the first. */
    public void testSingleBatchStillValidatesNameAfterFirstItem() throws IOException {
        EscfBatch batch = buildBatch(2);
        IndexMetadata md = plainMetadata("myindex", 1);
        IndexMetadata other = plainMetadata("otherindex", 1);
        ProjectMetadata project = project(md, other);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(rowRequest("myindex", batch, 0));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        sharder.prepareRouting(first, md.getIndex(), routing, project).recordRoutedShard(first, first.route(routing));

        IndexRequest rewritten = rowRequest("otherindex", batch, 1);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> sharder.prepareRouting(rewritten, other.getIndex(), IndexRouting.fromIndexMetadata(other), project)
        );
        assertThat(e.getMessage(), containsString("no pre-built batch was supplied under that name"));
        sharder.close();
    }

    public void testValidateRejectsRowBearingItemWithNoBatch() throws IOException {
        EscfBatch batch = buildBatch(1);
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        var requestsByShard = Map.of(shardId, List.of(new BulkItemRequest(0, rowRequest("myindex", batch, 0))));

        var e = expectThrows(IllegalStateException.class, () -> SourceBatchSharder.validateBatchAlignment(requestsByShard, Map.of()));
        assertThat(e.getMessage(), containsString("would be indexed with an empty source"));
    }

    public void testValidateRejectsRowCountMismatch() throws IOException {
        EscfBatch batch = buildBatch(2);
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        // Three items for a two-row batch.
        List<BulkItemRequest> items = List.of(
            new BulkItemRequest(0, rowRequest("myindex", batch, 0)),
            new BulkItemRequest(1, rowRequest("myindex", batch, 1)),
            new BulkItemRequest(2, rowRequest("myindex", batch, 1))
        );
        var e = expectThrows(
            IllegalStateException.class,
            () -> SourceBatchSharder.validateBatchAlignment(Map.of(shardId, items), Map.of(shardId, batch))
        );
        assertThat(e.getMessage(), containsString("does not align with its items"));
    }

    public void testValidatePassesForInlineSourceItems() {
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        var items = List.of(new BulkItemRequest(0, new IndexRequest("myindex").source(new HashMap<>())));
        SourceBatchSharder.validateBatchAlignment(Map.of(shardId, items), Map.of());
    }
}
