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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BatchModeRouterTests extends ESTestCase {

    /** Data stream name used by tests that exercise the single-backing-index TSDB path. */
    private static final String DATA_STREAM = "metrics-app-default";
    /** Fixed epoch for backing index names so tests do not depend on wall-clock time. */
    private static final long EPOCH_MILLIS = 1704067200000L; // 2024-01-01T00:00:00Z

    private static final String GEN_1_START = "2024-01-01T00:00:00Z";
    private static final String GEN_1_END = "2024-06-01T00:00:00Z";

    private static final Instant IN_GEN_1 = Instant.parse("2024-03-01T00:00:00Z");

    /** Builds a plain {@link IndexMetadata} with no routing path (Unpartitioned strategy). */
    private static IndexMetadata plainMetadata(String name, int shards) {
        return IndexMetadata.builder(name).settings(indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)).build();
    }

    /**
     * Builds a TSDB backing index whose routing strategy is
     * {@link IndexRouting.ExtractFromSource.ForIndexDimensions}: time_series mode plus a non-empty
     * {@code index.dimensions}, which is what selects that strategy in
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
     * Builds sourceless {@link IndexRequest}s referencing rows {@code 0..numDocs-1} of {@code batch}
     * and attaches the batch under {@code batchKey} — the name the requests target, which is what
     * the router keys on.
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
     * Mirror of {@link BulkOperation}'s shard grouping loop: resolves the concrete write index and
     * routing for each item, then delegates the full routing step — pre-process, routing decision,
     * post-process, and batch bookkeeping — to {@link BatchModeRouter#route}. Calls
     * {@link BatchModeRouter#buildGrouping} after the scan, matching the production path.
     *
     * @param skipRows rows to drop before routing, standing in for items that fail validation in the
     *                 real loop
     */
    private static Map<ShardId, List<BulkItemRequest>> routeAll(
        BatchModeRouter router,
        BulkRequest bulkRequest,
        ProjectMetadata project,
        Set<Integer> skipRows
    ) {
        router.preResolveTimestamps(project, bulkRequest.requests());
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
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
            router.route(item, request, abstraction, concreteIndex, routing, project, requestsByShard);
        }
        return router.buildGrouping(requestsByShard, (item, e) -> { throw new AssertionError("unexpected routing failure", e); });
    }

    private static Map<ShardId, List<BulkItemRequest>> routeAll(BatchModeRouter router, BulkRequest bulkRequest, ProjectMetadata project) {
        return routeAll(router, bulkRequest, project, Set.of());
    }

    /** Asserts every shard's items map 1:1 and in order onto its batch's rows. */
    private static void assertShardsAligned(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        BatchModeRouter.validateBatchAlignment(requestsByShard, shardBatches);
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
        assertThat(BatchModeRouter.create(new BulkRequest(), true), nullValue());
    }

    public void testCreateReturnsNullWhenEmptyBatchMap() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of());
        assertThat(BatchModeRouter.create(request, true), nullValue());
    }

    /**
     * A {@link SourceBatch} that is not an {@link EscfBatch} is unreachable through any production
     * code path today, so a stub is the only way to exercise the guard. It throws from every method
     * to make it obvious if anything other than the {@code instanceof} check ever touches it.
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
        public int estimatedBytes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    public void testCreateThrowsForNonEscfBatch() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("myindex", new NotAnEscfBatch()));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(request, true));
        assertThat(e.getMessage(), containsString("must be an EscfBatch"));
    }

    /**
     * Step-1 limit: exactly one pre-built batch per bulk. A second batch name triggers an immediate
     * rejection at create time with a message pointing to the upcoming follow-up.
     */
    public void testRejectsMultipleBatches() throws IOException {
        EscfBatch batchA = buildBatch(1);
        EscfBatch batchB = buildBatch(1);
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("index-a", batchA, "index-b", batchB));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(request, true));
        assertThat(e.getMessage(), containsString("at most one is supported in step 1"));
    }

    public void testSingleShardAllRowsRouted() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        assertThat(router, notNullValue());
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        assertThat(result.size(), equalTo(1));
        SourceBatch shardBatch = result.get(new ShardId(md.getIndex(), 0));
        assertThat(shardBatch, notNullValue());
        assertThat(shardBatch.docCount(), equalTo(numDocs));
        assertShardsAligned(requestsByShard, result);
        router.close();
    }

    public void testMultiShardRowsAlignWithItems() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        int numShards = randomIntBetween(2, 5);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", numShards);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        assertShardsAligned(requestsByShard, result);
        assertThat(result.size(), equalTo(requestsByShard.size()));
        router.close();
    }

    /**
     * If some (but not all) rows are dropped before routing, {@code completeDeferredRouting} must
     * fail rather than silently produce a misaligned batch. Discard-bucket support will be added in
     * a follow-up.
     */
    public void testThrowsWhenSomeRowsDropped() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        int numShards = randomIntBetween(1, 4);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", numShards));

        // Drop between 1 and numDocs-1 rows so that routedCount > 0 and < docCount.
        int dropCount = randomIntBetween(1, numDocs - 1);
        Set<Integer> dropped = new HashSet<>();
        while (dropped.size() < dropCount) {
            dropped.add(randomIntBetween(0, numDocs - 1));
        }
        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(IllegalStateException.class, () -> routeAll(router, bulkRequest, project, dropped));
        assertThat(e.getMessage(), containsString("not yet supported"));
        router.close();
    }

    public void testAllRowsDroppedProducesNoBatches() throws IOException {
        EscfBatch batch = buildBatch(5);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, 5);
        ProjectMetadata project = project(plainMetadata("myindex", 2));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project, Set.of(0, 1, 2, 3, 4));
        assertTrue(requestsByShard.isEmpty());
        assertTrue(router.shardBatches().isEmpty());
        router.close();
    }

    /**
     * The failure-store redirect pass re-enters {@code executeBulkRequestsByShard}, so
     * {@code shardBatches()} can be called a second time. It must not re-scatter: the first call's
     * batches are already attached to in-flight shard requests and their items already point at
     * shard-local rows.
     */
    public void testSecondShardBatchesCallIsANoOp() throws IOException {
        int numDocs = 12;
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", 3));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> first = router.shardBatches();
        assertFalse(first.isEmpty());

        List<Integer> rowsAfterFirst = bulkRequest.requests.stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList();
        assertThat(router.shardBatches(), equalTo(Map.of()));
        List<Integer> rowsAfterSecond = bulkRequest.requests.stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList();
        assertThat(rowsAfterSecond, equalTo(rowsAfterFirst));
        assertShardsAligned(requestsByShard, first);
        router.close();
    }

    public void testRejectsItemWithoutSourceRow() throws IOException {
        EscfBatch batch = buildBatch(1);
        // Item has inline source — no source-row reference.
        IndexRequest request = new IndexRequest("myindex").id("doc-0").source(new HashMap<>());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(bulkRequest, true));
        assertThat(e.getMessage(), containsString("must carry a source-row reference"));
    }

    public void testRejectsRowBearingItemWithNoBatchForItsName() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        IndexMetadata other = plainMetadata("otherindex", 1);
        ProjectMetadata project = project(plainMetadata("myindex", 1), other);
        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);

        // Carries a row but targets a name with no batch — e.g. because something rewrote _index.
        IndexRequest request = rowRequest("otherindex", batch, 0);
        IndexAbstraction ia = project.getIndicesLookup().get(request.index());
        BulkItemRequest item = new BulkItemRequest(0, request);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> router.route(item, request, ia, other.getIndex(), IndexRouting.fromIndexMetadata(other), project, new HashMap<>())
        );
        assertThat(e.getMessage(), containsString("no pre-built batch was supplied under that name"));
        router.close();
    }

    public void testRejectsInlineItemForAnUnbatchedName() throws IOException {
        EscfBatch batch = buildBatch(1);
        // Inline source in a bulk that carries batches: its shard's rows could not line up with its items.
        IndexRequest request = new IndexRequest("otherindex").id("doc-0").source(new HashMap<>());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(bulkRequest, true));
        assertThat(e.getMessage(), containsString("must carry a source-row reference"));
    }

    public void testRejectsNonIndexRequestItem() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest("myindex", "doc-0"));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(bulkRequest, true));
        assertThat(e.getMessage(), containsString("cannot be mixed with pre-built source batches"));
    }

    /**
     * Rows whose timestamps fall in different TSDB backing-index time ranges resolve to different
     * concrete write indices (two generations of the same data stream). Multi-index fan-out is now
     * supported, so routing must succeed and each backing index must receive exactly the rows that
     * belong to its time range.
     */
    public void testRowsSpanTwoBackingIndices() throws IOException {
        String gen2End = "2025-01-01T00:00:00Z";
        IndexMetadata gen1 = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        IndexMetadata gen2 = tsdbBackingIndex(2, 1, GEN_1_END, gen2End);
        ProjectMetadata project = projectWithDataStream(gen1, gen2);

        EscfBatch batch = buildBatch(2);
        Instant inGen2 = Instant.parse("2024-09-01T00:00:00Z");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1)); // row 0 → gen 1
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 1, inGen2));   // row 1 → gen 2
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> shardBatches = router.shardBatches();

        // Each backing index has one shard and must receive exactly one row.
        assertThat("expected two shards (one per backing index)", shardBatches.size(), equalTo(2));
        ShardId gen1Shard = new ShardId(gen1.getIndex(), 0);
        ShardId gen2Shard = new ShardId(gen2.getIndex(), 0);
        assertThat("gen1 shard must have one row", shardBatches.get(gen1Shard).docCount(), equalTo(1));
        assertThat("gen2 shard must have one row", shardBatches.get(gen2Shard).docCount(), equalTo(1));
        assertShardsAligned(requestsByShard, shardBatches);
        router.close();
    }

    /**
     * Two backing indices with different shard counts: gen 1 has 2 shards, gen 2 has 3 shards,
     * giving 5 total partitions. Each shard that receives rows must be aligned with its items.
     */
    public void testTwoBackingIndicesScatterIntoFiveShards() throws IOException {
        String gen2End = "2025-01-01T00:00:00Z";
        IndexMetadata gen1 = tsdbBackingIndex(1, 2, GEN_1_START, GEN_1_END); // 2 shards
        IndexMetadata gen2 = tsdbBackingIndex(2, 3, GEN_1_END, gen2End);      // 3 shards
        ProjectMetadata project = projectWithDataStream(gen1, gen2);

        // 6 rows: rows 0–2 in gen 1's time range, rows 3–4 in gen 2's time range, row 5 back in gen 1.
        // The final gen-1 row exercises non-contiguous target assignments in rowTargets[].
        int numRows = 6;
        EscfBatch batch = buildBatch(numRows);
        Instant inGen2 = Instant.parse("2024-09-01T00:00:00Z");
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 3; i++) {
            bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, i, IN_GEN_1));
        }
        for (int i = 3; i < 5; i++) {
            bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, i, inGen2));
        }
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 5, IN_GEN_1));
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> shardBatches = router.shardBatches();

        // All rows must be accounted for across both backing indices.
        int totalRows = shardBatches.values().stream().mapToInt(SourceBatch::docCount).sum();
        assertThat("all rows must be accounted for", totalRows, equalTo(numRows));
        // No shard may belong to the wrong backing index.
        for (ShardId sid : shardBatches.keySet()) {
            boolean isGen1 = sid.getIndex().equals(gen1.getIndex());
            boolean isGen2 = sid.getIndex().equals(gen2.getIndex());
            assertTrue("shard " + sid + " belongs to neither backing index", isGen1 || isGen2);
        }
        assertShardsAligned(requestsByShard, shardBatches);
        router.close();
    }

    public void testRejectsNonMonotonicRowIndex() throws IOException {
        EscfBatch batch = buildBatch(3);
        BulkRequest bulkRequest = new BulkRequest();
        // Items in reverse row order — should fail on the second item.
        for (int i = 2; i >= 0; i--) {
            bulkRequest.add(rowRequest("myindex", batch, i));
        }
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        ProjectMetadata project = project(plainMetadata("myindex", 1));
        var e = expectThrows(IllegalArgumentException.class, () -> routeAll(router, bulkRequest, project));
        assertThat(e.getMessage(), containsString("not strictly greater"));
        router.close();
    }

    /**
     * A provided batch without pre-computed {@code _tsid} is now supported: the columnar tsid
     * calculator derives the tsid from the dimension column ({@code "dim"}) in the batch. The
     * computed tsid must match what {@link IndexRouting.ExtractFromSource.ForIndexDimensions#buildTsid}
     * produces from the same source document.
     */
    public void testProvidedBatchWithoutTsidRoutesViaColumnarCalculation() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        assertThat(routing, instanceOf(IndexRouting.ExtractFromSource.ForIndexDimensions.class));
        IndexRouting.ExtractFromSource.ForIndexDimensions dims = (IndexRouting.ExtractFromSource.ForIndexDimensions) routing;
        ProjectMetadata project = projectWithDataStream(md);

        // batch has {"dim": "d0", "val": 0} — "dim" is the declared dimension field.
        Docs docs = buildDocs(1);
        EscfBatch batch = docs.batch();
        IndexRequest request = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        request.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        request.setTimeSeriesTimestamp(IN_GEN_1); // no pre-computed tsid
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);

        // Tsid must have been computed and attached to the request.
        assertThat(request.tsid(), notNullValue());
        BytesRef expectedTsid = dims.buildTsid(XContentType.JSON, docs.sources().get(0));
        assertThat(request.tsid(), equalTo(expectedTsid));

        assertShardsAligned(requestsByShard, router.shardBatches());
        router.close();
    }

    public void testForIndexDimensionsWithTsidSucceeds() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1));
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        assertShardsAligned(requestsByShard, router.shardBatches());
        router.close();
    }

    public void testSingleShardPassthroughHandsSourceBatchThrough() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        Docs docs = buildDocs(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", docs.batch(), numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

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
        router.close();
    }

    /**
     * Even a single-shard index throws when a row is dropped, because the passthrough fast path
     * requires all rows to be present. The exception comes from {@code completeDeferredRouting},
     * which is called by {@code routeAll} after the scan.
     */
    public void testSingleShardWithDroppedRowThrows() throws IOException {
        int numDocs = randomIntBetween(2, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(
            IllegalStateException.class,
            () -> routeAll(router, bulkRequest, project, Set.of(randomIntBetween(0, numDocs - 1)))
        );
        assertThat(e.getMessage(), containsString("not yet supported"));
        router.close();
    }

    /** More than one shard means the rows genuinely have to be split, whatever they happened to route to. */
    public void testMultiShardDoesNotPassThrough() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", randomIntBetween(2, 5)));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        for (Map.Entry<ShardId, SourceBatch> entry : result.entrySet()) {
            assertNotSame("shard " + entry.getKey() + " was handed the whole batch", batch, entry.getValue());
        }
        assertShardsAligned(requestsByShard, result);
        router.close();
    }

    /**
     * A batch where some items carry a pre-computed {@code _tsid} and others do not violates the
     * all-or-none invariant enforced by
     * {@link IndexRouting.ExtractFromSource.ForIndexDimensions#indexShard(IndexRequest[], org.elasticsearch.sourcebatch.SourceBatch)}.
     * The violation is detected during {@code completeDeferredRouting}.
     */
    public void testMixedTsidBatchIsRejected() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(2);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1));  // has pre-set tsid
        IndexRequest withoutTsid = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        withoutTsid.indexSource().setSourceRow(batch, 1, XContentType.JSON);
        withoutTsid.setTimeSeriesTimestamp(IN_GEN_1); // but no tsid
        bulkRequest.add(withoutTsid);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        // The all-or-none tsid check fires during buildGrouping: both items are reported via the
        // onItemFailure callback and the returned grouping is empty.
        List<Exception> failures = new ArrayList<>();
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
        int slot = 0;
        for (DocWriteRequest<?> req : bulkRequest.requests) {
            IndexRequest ir = (IndexRequest) req;
            BulkItemRequest item = new BulkItemRequest(slot++, ir);
            IndexAbstraction ia = project.getIndicesLookup().get(ir.index());
            Index idx = ir.getConcreteWriteIndex(ia, project);
            router.route(item, ir, ia, idx, IndexRouting.fromIndexMetadata(project.getIndexSafe(idx)), project, requestsByShard);
        }
        Map<ShardId, List<BulkItemRequest>> result = router.buildGrouping(requestsByShard, (item, e) -> failures.add(e));

        assertThat("grouping must be empty after tsid-consistency failure", result.isEmpty(), equalTo(true));
        assertThat("both items reported as failed", failures.size(), equalTo(2));
        assertThat(failures.get(0).getMessage(), containsString("Batch tsid consistency violation"));
        assertThat(router.shardBatches(), equalTo(Map.of()));
        router.close();
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

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        IndexAbstraction iaFirst = project.getIndicesLookup().get(first.index());
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
        router.route(new BulkItemRequest(0, first), first, iaFirst, md.getIndex(), routing, project, requestsByShard);

        IndexRequest rewritten = rowRequest("otherindex", batch, 1);
        IndexAbstraction iaOther = project.getIndicesLookup().get(rewritten.index());
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> router.route(
                new BulkItemRequest(1, rewritten),
                rewritten,
                iaOther,
                other.getIndex(),
                IndexRouting.fromIndexMetadata(other),
                project,
                requestsByShard
            )
        );
        assertThat(e.getMessage(), containsString("no pre-built batch was supplied under that name"));
        router.close();
    }

    public void testSingleShardSecondShardBatchesCallIsANoOp() throws IOException {
        int numDocs = randomIntBetween(1, 10);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", 1));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> first = router.shardBatches();
        assertThat(first.size(), equalTo(1));
        assertSame(batch, first.values().iterator().next());

        // Second call must not re-scatter the already-dispatched batch.
        assertThat(router.shardBatches(), equalTo(Map.of()));
        router.close();
    }

    public void testColumnarRoutingFailureReportsAllItemsViaCallback() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        // Build a batch with two rows; neither has a pre-set tsid, so ColumnarTsidCalculator runs.
        // By adding an explicit routing value on the request we trigger checkNoRouting inside
        // ForIndexDimensions.indexShard, which throws before computing any tsid.
        Docs docs = buildDocs(2);
        EscfBatch batch = docs.batch();
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 2; i++) {
            IndexRequest req = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
            req.indexSource().setSourceRow(batch, i, XContentType.JSON);
            req.setTimeSeriesTimestamp(IN_GEN_1);
            req.routing("forced-routing"); // triggers checkNoRouting inside indexShard
            bulkRequest.add(req);
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        List<BulkItemRequest> failedItems = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
        int slot = 0;
        for (DocWriteRequest<?> req : bulkRequest.requests) {
            IndexRequest ir = (IndexRequest) req;
            BulkItemRequest item = new BulkItemRequest(slot++, ir);
            IndexAbstraction ia = project.getIndicesLookup().get(ir.index());
            Index idx = ir.getConcreteWriteIndex(ia, project);
            router.route(item, ir, ia, idx, IndexRouting.fromIndexMetadata(project.getIndexSafe(idx)), project, requestsByShard);
        }
        Map<ShardId, List<BulkItemRequest>> result = router.buildGrouping(requestsByShard, (item, e) -> {
            failedItems.add(item);
            failures.add(e);
        });

        assertThat("grouping must be empty after routing failure", result.isEmpty(), equalTo(true));
        assertThat("all items reported as failed", failedItems.size(), equalTo(2));
        for (Exception e : failures) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
        }
        // shardBatches must also be empty: scattered flag was set to prevent a stale scatter.
        assertThat(router.shardBatches(), equalTo(Map.of()));
        router.close();
    }

    /**
     * A TSDB batch where every item lacks a pre-computed {@code _tsid} routes correctly via
     * {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator}: the calculator derives the
     * tsid from the dimension column and {@link BatchModeRouter#buildGrouping} populates the
     * grouping without any failure callbacks being fired.
     */
    public void testColumnarRoutingSucceedsForBatchWithoutPrecomputedTsid() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        Docs docs = buildDocs(randomIntBetween(1, 10));
        EscfBatch batch = docs.batch();
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < docs.sources().size(); i++) {
            IndexRequest req = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
            req.indexSource().setSourceRow(batch, i, XContentType.JSON);
            req.setTimeSeriesTimestamp(IN_GEN_1); // no pre-set tsid
            bulkRequest.add(req);
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);

        assertThat("all rows must land on exactly one shard in a 1-shard index", requestsByShard.size(), equalTo(1));
        assertThat(requestsByShard.values().iterator().next().size(), equalTo(docs.sources().size()));
        assertShardsAligned(requestsByShard, router.shardBatches());
        router.close();
    }

    /**
     * {@link BatchModeRouter#preResolveTimestamps} reads epoch-millisecond LONG values from the
     * batch's {@code @timestamp} column and caches the canonical (second-truncated) bound on each
     * request. A sub-second input verifies that the truncation applies.
     */
    public void testPreResolveTimestampsFromLongColumn() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        Instant ts0 = Instant.parse("2024-02-01T12:34:56.789Z"); // sub-second precision
        Instant ts1 = IN_GEN_1;

        EscfBatch batch;
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (Instant ts : List.of(ts0, ts1)) {
                XContentBuilder doc = JsonXContent.contentBuilder();
                doc.startObject();
                doc.field("dim", "d");
                doc.field("@timestamp", ts.toEpochMilli());
                doc.endObject();
                encoder.parseToScratch(BytesReference.bytes(doc), XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            batch = encoder.buildPartition(0);
        }

        IndexRequest req0 = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        req0.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        IndexRequest req1 = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        req1.indexSource().setSourceRow(batch, 1, XContentType.JSON);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(req0);
        bulkRequest.add(req1);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.preResolveTimestamps(project, bulkRequest.requests());

        assertThat(req0.getTimeSeriesTimestamp(), equalTo(DataStream.getCanonicalTimestampBound(ts0)));
        assertThat(req1.getTimeSeriesTimestamp(), equalTo(DataStream.getCanonicalTimestampBound(ts1)));
        router.close();
    }

    /**
     * {@link BatchModeRouter#preResolveTimestamps} reads an ISO-8601 STRING value from the batch's
     * {@code @timestamp} column, parses it, and caches the canonical bound on the request.
     */
    public void testPreResolveTimestampsFromStringColumn() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch;
        try (EscfEncoder encoder = new EscfEncoder()) {
            XContentBuilder doc = JsonXContent.contentBuilder();
            doc.startObject();
            doc.field("dim", "d0");
            doc.field("@timestamp", IN_GEN_1.toString());
            doc.endObject();
            encoder.parseToScratch(BytesReference.bytes(doc), XContentType.JSON, LeafSink.NO_OP);
            encoder.commitScratchTo(0);
            batch = encoder.buildPartition(0);
        }

        IndexRequest req = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        req.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(req);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.preResolveTimestamps(project, bulkRequest.requests());

        assertThat(req.getTimeSeriesTimestamp(), equalTo(DataStream.getCanonicalTimestampBound(IN_GEN_1)));
        router.close();
    }

    /**
     * {@link BatchModeRouter#preResolveTimestamps} handles a UNION {@code @timestamp} column.
     * Storing a LONG in one row and a STRING in another causes the ESCF encoder to promote the
     * column to UNION. Both rows must resolve to the correct canonical bound regardless of type.
     */
    public void testPreResolveTimestampsFromUnionColumn() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        Instant ts0 = IN_GEN_1;
        Instant ts1 = Instant.parse("2024-04-01T00:00:00Z");

        EscfBatch batch;
        try (EscfEncoder encoder = new EscfEncoder()) {
            // Row 0: LONG timestamp — seeds the column as LONG.
            XContentBuilder doc0 = JsonXContent.contentBuilder();
            doc0.startObject();
            doc0.field("dim", "d");
            doc0.field("@timestamp", ts0.toEpochMilli());
            doc0.endObject();
            encoder.parseToScratch(BytesReference.bytes(doc0), XContentType.JSON, LeafSink.NO_OP);
            encoder.commitScratchTo(0);

            // Row 1: STRING timestamp — promotes the column to UNION.
            XContentBuilder doc1 = JsonXContent.contentBuilder();
            doc1.startObject();
            doc1.field("dim", "d");
            doc1.field("@timestamp", ts1.toString());
            doc1.endObject();
            encoder.parseToScratch(BytesReference.bytes(doc1), XContentType.JSON, LeafSink.NO_OP);
            encoder.commitScratchTo(0);

            batch = encoder.buildPartition(0);
        }

        IndexRequest req0 = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        req0.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        IndexRequest req1 = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        req1.indexSource().setSourceRow(batch, 1, XContentType.JSON);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(req0);
        bulkRequest.add(req1);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.preResolveTimestamps(project, bulkRequest.requests());

        assertThat(req0.getTimeSeriesTimestamp(), equalTo(DataStream.getCanonicalTimestampBound(ts0)));
        assertThat(req1.getTimeSeriesTimestamp(), equalTo(DataStream.getCanonicalTimestampBound(ts1)));
        router.close();
    }

    /**
     * A TSDB batch with no {@code @timestamp} column throws during
     * {@link BatchModeRouter#preResolveTimestamps}: no column means no backing-index selection is
     * possible, so the batch is rejected early.
     */
    public void testPreResolveTimestampsMissingColumnThrows() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        // buildBatch produces {"dim": "d0", "val": 0} with no @timestamp.
        EscfBatch batch = buildBatch(1);
        IndexRequest req = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        req.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(req);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(IllegalArgumentException.class, () -> router.preResolveTimestamps(project, bulkRequest.requests()));
        assertThat(e.getMessage(), containsString("@timestamp"));
        router.close();
    }

    /**
     * A batch where some requests carry a pre-set timestamp and some do not violates the all-or-none
     * invariant: either the producer supplies timestamps for every row, or none. A mix is rejected
     * during {@link BatchModeRouter#preResolveTimestamps}.
     */
    public void testPreResolveTimestampsMixedPresetThrows() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(2);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1)); // has pre-set timestamp
        IndexRequest withoutTs = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        withoutTs.indexSource().setSourceRow(batch, 1, XContentType.JSON);
        bulkRequest.add(withoutTs); // no timestamp
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(IllegalArgumentException.class, () -> router.preResolveTimestamps(project, bulkRequest.requests()));
        assertThat(e.getMessage(), containsString("mix of requests"));
        router.close();
    }

    public void testValidateRejectsRowBearingItemWithNoBatch() throws IOException {
        EscfBatch batch = buildBatch(1);
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        var requestsByShard = Map.of(shardId, List.of(new BulkItemRequest(0, rowRequest("myindex", batch, 0))));

        var e = expectThrows(IllegalStateException.class, () -> BatchModeRouter.validateBatchAlignment(requestsByShard, Map.of()));
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
            () -> BatchModeRouter.validateBatchAlignment(Map.of(shardId, items), Map.of(shardId, batch))
        );
        assertThat(e.getMessage(), containsString("does not align with its items"));
    }

    public void testValidatePassesForInlineSourceItems() {
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        var items = List.of(new BulkItemRequest(0, new IndexRequest("myindex").source(new HashMap<>())));
        BatchModeRouter.validateBatchAlignment(Map.of(shardId, items), Map.of());
    }
}
