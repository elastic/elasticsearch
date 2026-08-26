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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SourceBatchSharderTests extends ESTestCase {

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
     * the sharder keys on.
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
     * Mirror of {@link BulkOperation}'s shard grouping loop for the parts the sharder participates
     * in: resolve the concrete write index of each item, then
     * preRouting → prepareRouting → route → postRouting → recordRoutedShard, accumulating items per
     * shard in bulk order.
     *
     * @param skipRows rows to drop before routing, standing in for items that fail validation in the
     *                 real loop
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
            if (sharder != null) {
                sharder.prepareRouting(request, concreteIndex, routing, project);
            }
            int shardId = request.route(routing);
            request.postRoutingProcess(routing);
            if (sharder != null) {
                sharder.recordRoutedShard(request, shardId);
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
        public void close() {}
    }

    public void testCreateThrowsForNonEscfBatch() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("myindex", new NotAnEscfBatch()));
        var e = expectThrows(IllegalArgumentException.class, () -> SourceBatchSharder.create(request));
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
        var e = expectThrows(IllegalArgumentException.class, () -> SourceBatchSharder.create(request));
        assertThat(e.getMessage(), containsString("at most one is supported in step 1"));
    }

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
     * If some (but not all) rows are dropped before routing, {@code shardBatches()} must fail
     * rather than silently produce a misaligned batch. Discard-bucket support will be added in a
     * follow-up.
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
        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        routeAll(sharder, bulkRequest, project, dropped);
        var e = expectThrows(IllegalStateException.class, sharder::shardBatches);
        assertThat(e.getMessage(), containsString("not yet supported"));
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

    /**
     * Step-1 limit: one batch may only resolve to one concrete write index. This is the restriction
     * that prevents using pre-built batches with TSDB data streams spanning two backing indices.
     * Support for multi-index fan-out will be added in a follow-up.
     */
    public void testRejectsSecondConcreteIndex() throws IOException {
        // Two plain indices; the items target the same batch name but resolve to different
        // concrete write indices because the metadata has no alias pointing both to the same one.
        EscfBatch batch = buildBatch(2);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(rowRequest("myindex", batch, 0));
        bulkRequest.add(rowRequest("myindex", batch, 1));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        IndexMetadata mdA = plainMetadata("myindex", 1);
        // Simulate a second concrete index by using a different Index object (different UUID).
        Index concreteA = mdA.getIndex();
        IndexMetadata mdB = IndexMetadata.builder("myindex-alt")
            .settings(indexSettings("myindex-alt").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .build();
        Index concreteB = mdB.getIndex();
        ProjectMetadata project = project(mdA, mdB);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        IndexRouting routingA = IndexRouting.fromIndexMetadata(mdA);
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        sharder.prepareRouting(first, concreteA, routingA, project);
        sharder.recordRoutedShard(first, 0);

        // The second item resolves to a different concrete index — must be rejected.
        IndexRequest second = (IndexRequest) bulkRequest.requests.get(1);
        IndexRouting routingB = IndexRouting.fromIndexMetadata(mdB);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.prepareRouting(second, concreteB, routingB, project));
        assertThat(e.getMessage(), containsString("not yet supported"));
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
        sharder.prepareRouting(first, md.getIndex(), routing, project);
        sharder.recordRoutedShard(first, 0);

        IndexRequest second = (IndexRequest) bulkRequest.requests.get(1);
        sharder.prepareRouting(second, md.getIndex(), routing, project);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.recordRoutedShard(second, 0));
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
        sharder.prepareRouting(request, md.getIndex(), IndexRouting.fromIndexMetadata(md), project);
        var e = expectThrows(IllegalStateException.class, () -> sharder.recordRoutedShard(request, 2));
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

    /**
     * Even a single-shard index throws when a row is dropped, because the passthrough fast path
     * requires all rows to be present.
     */
    public void testSingleShardWithDroppedRowThrows() throws IOException {
        int numDocs = randomIntBetween(2, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        routeAll(sharder, bulkRequest, project, Set.of(randomIntBetween(0, numDocs - 1)));
        var e = expectThrows(IllegalStateException.class, sharder::shardBatches);
        assertThat(e.getMessage(), containsString("not yet supported"));
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

    /** Resolving an already-bound index must not skip the per-item {@code _tsid} check. */
    public void testBoundIndexStillValidatesTsid() throws IOException {
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
        // The first item binds the index; the second resolves the same one and must still be rejected.
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
        sharder.prepareRouting(first, md.getIndex(), routing, project);
        sharder.recordRoutedShard(first, first.route(routing));

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
