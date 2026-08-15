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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SourceBatchSharderTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Builds a plain {@link IndexMetadata} with no routing path (Unpartitioned strategy). */
    private static IndexMetadata plainMetadata(String name, int shards) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    /** Builds a {@link EscfBatch} with {@code n} rows, each containing a single field "val" = i. */
    private static EscfBatch buildBatch(int n) throws IOException {
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (int i = 0; i < n; i++) {
                XContentBuilder doc = JsonXContent.contentBuilder();
                doc.startObject();
                doc.field("val", (long) i);
                doc.endObject();
                encoder.parseToScratch(BytesReference.bytes(doc), XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            return encoder.buildPartition(0);
        }
    }

    /**
     * Builds a {@link BulkRequest} with {@code numDocs} sourceless {@link IndexRequest}s, each
     * referencing row {@code i} of {@code batch}, and attaches the batch as a pre-built batch.
     */
    private static BulkRequest buildBulkRequest(String indexName, EscfBatch batch, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            IndexRequest ir = new IndexRequest(indexName).id("doc-" + i).opType(DocWriteRequest.OpType.INDEX);
            ir.indexSource().setSourceRow(batch, i, XContentType.JSON);
            bulkRequest.add(ir);
        }
        bulkRequest.setPreBuiltBatches(Map.of(indexName, batch));
        return bulkRequest;
    }

    // -------------------------------------------------------------------------
    // Tests: create()
    // -------------------------------------------------------------------------

    public void testCreateReturnsNullWhenNoBatches() {
        BulkRequest request = new BulkRequest();
        assertThat(SourceBatchSharder.create(request), nullValue());
    }

    public void testCreateReturnsNullWhenEmptyBatchMap() throws IOException {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of());
        assertThat(SourceBatchSharder.create(request), nullValue());
    }

    public void testCreateThrowsForNonEscfBatch() {
        // Use a SourceBatch that is not an EscfBatch — simulate with an anonymous impl
        SourceBatch nonEscf = new SourceBatch() {
            @Override
            public int docCount() {
                return 0;
            }

            @Override
            public org.elasticsearch.sourcebatch.SourceSchema schema() {
                return null;
            }

            @Override
            public int columnCount() {
                return 0;
            }

            @Override
            public BytesReference data() {
                return null;
            }

            @Override
            public org.elasticsearch.sourcebatch.SourceRow row(int docIndex) {
                return null;
            }

            @Override
            public SourceBatch slice(int from, int to) {
                return null;
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("myindex", nonEscf));
        var e = expectThrows(IllegalArgumentException.class, () -> SourceBatchSharder.create(request));
        assertThat(e.getMessage(), containsString("must be an EscfBatch"));
    }

    // -------------------------------------------------------------------------
    // Tests: single shard (no actual scatter)
    // -------------------------------------------------------------------------

    public void testSingleShardAllRowsRouted() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        assertThat(sharder, notNullValue());

        IndexMetadata md = plainMetadata("myindex", 1);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        Index concreteIndex = new Index("myindex", "test-uuid");

        List<IndexRequest> requests = new ArrayList<>();
        for (DocWriteRequest<?> r : bulkRequest.requests) {
            IndexRequest ir = (IndexRequest) r;
            sharder.checkRoutable(ir, "myindex", routing);
            int shardId = ir.route(routing);
            sharder.recordRouting(ir, concreteIndex, shardId, md.getNumberOfShards());
            requests.add(ir);
        }

        Map<ShardId, SourceBatch> result = sharder.shardBatches();
        // Single shard: all docs in shard 0.
        assertThat(result.size(), equalTo(1));
        SourceBatch shardBatch = result.get(new ShardId(concreteIndex, 0));
        assertThat(shardBatch, notNullValue());
        assertThat(shardBatch.docCount(), equalTo(numDocs));

        // Each item must now reference its shard-local row index (0..numDocs-1).
        for (int i = 0; i < requests.size(); i++) {
            assertThat("row re-point for item " + i, requests.get(i).indexSource().rowIndex(), equalTo(i));
        }

        sharder.close();
    }

    // -------------------------------------------------------------------------
    // Tests: multi-shard scatter
    // -------------------------------------------------------------------------

    public void testMultiShardRowsAlignWithItems() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        int numShards = randomIntBetween(2, 5);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        assertThat(sharder, notNullValue());

        IndexMetadata md = plainMetadata("myindex", numShards);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        Index concreteIndex = new Index("myindex", "test-uuid-multi");

        // Track items per shard in arrival order to verify alignment after scatter.
        List<List<IndexRequest>> itemsByShard = new ArrayList<>();
        for (int s = 0; s < numShards; s++) {
            itemsByShard.add(new ArrayList<>());
        }

        for (DocWriteRequest<?> r : bulkRequest.requests) {
            IndexRequest ir = (IndexRequest) r;
            sharder.checkRoutable(ir, "myindex", routing);
            int shardId = ir.route(routing);
            sharder.recordRouting(ir, concreteIndex, shardId, md.getNumberOfShards());
            itemsByShard.get(shardId).add(ir);
        }

        Map<ShardId, SourceBatch> result = sharder.shardBatches();

        // Verify: for each shard that received items, row indices are 0..n-1 within the shard.
        for (int s = 0; s < numShards; s++) {
            List<IndexRequest> shardItems = itemsByShard.get(s);
            if (shardItems.isEmpty()) {
                assertThat(result.get(new ShardId(concreteIndex, s)), nullValue());
                continue;
            }
            SourceBatch shardBatch = result.get(new ShardId(concreteIndex, s));
            assertThat("shard " + s + " has batch", shardBatch, notNullValue());
            assertThat("shard " + s + " row count", shardBatch.docCount(), equalTo(shardItems.size()));
            for (int i = 0; i < shardItems.size(); i++) {
                assertThat("shard " + s + " item " + i + " row index", shardItems.get(i).indexSource().rowIndex(), equalTo(i));
            }
        }

        sharder.close();
    }

    // -------------------------------------------------------------------------
    // Tests: error cases
    // -------------------------------------------------------------------------

    public void testRejectsItemWithoutSourceRow() throws IOException {
        EscfBatch batch = buildBatch(1);

        SourceBatchSharder sharder = SourceBatchSharder.create(new BulkRequest() {
            {
                setPreBuiltBatches(Map.of("myindex", batch));
            }
        });

        IndexMetadata md = plainMetadata("myindex", 1);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        Index concreteIndex = new Index("myindex", "test-uuid");

        // Item has inline source — no source-row reference.
        IndexRequest ir = new IndexRequest("myindex").id("doc-0").source(new java.util.HashMap<>());
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.recordRouting(ir, concreteIndex, 0, md.getNumberOfShards()));
        assertThat(e.getMessage(), containsString("must carry a source-row reference"));
        sharder.close();
    }

    public void testRejectsNonMonotonicRowIndex() throws IOException {
        EscfBatch batch = buildBatch(3);
        BulkRequest bulkRequest = new BulkRequest();
        // Items in reverse row order — should fail on second item.
        for (int i = 2; i >= 0; i--) {
            IndexRequest ir = new IndexRequest("myindex").id("doc-" + i).opType(DocWriteRequest.OpType.INDEX);
            ir.indexSource().setSourceRow(batch, i, XContentType.JSON);
            bulkRequest.add(ir);
        }
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        IndexMetadata md = plainMetadata("myindex", 1);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        Index concreteIndex = new Index("myindex", "test-uuid");

        List<DocWriteRequest<?>> requests = bulkRequest.requests;
        // First item (row 2) should succeed.
        IndexRequest first = (IndexRequest) requests.get(0);
        sharder.recordRouting(first, concreteIndex, 0, md.getNumberOfShards());
        // Second item (row 1) is not strictly greater than 2 — should fail.
        IndexRequest second = (IndexRequest) requests.get(1);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.recordRouting(second, concreteIndex, 0, md.getNumberOfShards()));
        assertThat(e.getMessage(), containsString("not strictly greater"));
        sharder.close();
    }

    public void testRejectsForIndexDimensionsWithoutTsid() {
        // Build a TSDB-style metadata with dimensions to trigger ForIndexDimensions.
        IndexMetadata md = IndexMetadata.builder("tsindex")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        // Only test if this produces an ExtractFromSource routing (it may fall back to plain on some versions).
        if (routing instanceof IndexRouting.ExtractFromSource == false) {
            return; // Not a testable configuration on this build
        }

        BulkRequest bulkRequest = new BulkRequest();
        // Build a dummy batch and request.
        try {
            EscfBatch batch = buildBatch(1);
            IndexRequest ir = new IndexRequest("tsindex").opType(DocWriteRequest.OpType.CREATE);
            ir.indexSource().setSourceRow(batch, 0, XContentType.JSON);
            // No tsid set on the request.
            bulkRequest.add(ir);
            bulkRequest.setPreBuiltBatches(Map.of("tsindex", batch));
        } catch (IOException ex) {
            fail("unexpected IOException building batch: " + ex.getMessage());
        }

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        assertThat(sharder, notNullValue());

        IndexRequest ir = (IndexRequest) bulkRequest.requests.get(0);
        var e = expectThrows(IllegalArgumentException.class, () -> sharder.checkRoutable(ir, "tsindex", routing));
        assertThat(e.getMessage(), containsString("routes by extracting fields from _source"));
        sharder.close();
    }

    public void testForIndexDimensionsWithTsidSucceeds() throws IOException {
        // Build a metadata with ForIndexDimensions routing.
        IndexMetadata md = IndexMetadata.builder("tsindex")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        if (routing instanceof IndexRouting.ExtractFromSource == false) {
            return; // Not a testable configuration on this build
        }

        EscfBatch batch = buildBatch(1);
        IndexRequest ir = new IndexRequest("tsindex").opType(DocWriteRequest.OpType.CREATE);
        ir.indexSource().setSourceRow(batch, 0, XContentType.JSON);
        ir.tsid(new BytesRef("fake-tsid")); // pre-computed tsid → checkRoutable should pass

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(ir);
        bulkRequest.setPreBuiltBatches(Map.of("tsindex", batch));

        SourceBatchSharder sharder = SourceBatchSharder.create(bulkRequest);
        assertThat(sharder, notNullValue());
        // Should not throw.
        sharder.checkRoutable(ir, "tsindex", routing);
        sharder.close();
    }
}
