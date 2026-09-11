/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BulkBatchEncodersTests extends ESTestCase {

    private static final String INDEX = "my-index";
    private static final String DATA_STREAM = "metrics-app-default";
    private static final long EPOCH_MILLIS = 1704067200000L; // 2024-01-01T00:00:00Z

    private static final IndexNameExpressionResolver RESOLVER = TestIndexNameExpressionResolver.newInstance();

    // ---- helpers ----

    private static Settings.Builder indexSettings(String name) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
    }

    private static IndexMetadata plainMeta(String name) {
        return IndexMetadata.builder(name).settings(indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)).build();
    }

    private static ProjectMetadata singleIndexProject(String indexName) {
        return ProjectMetadata.builder(ProjectId.DEFAULT).put(plainMeta(indexName), false).build();
    }

    private static ProjectMetadata projectWithDataStream() {
        String gen1 = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1, EPOCH_MILLIS);
        IndexMetadata backing = IndexMetadata.builder(gen1)
            .settings(
                indexSettings(gen1).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.getName())
            )
            .build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT).put(backing, false);
        builder.put(DataStream.builder(DATA_STREAM, List.of(backing.getIndex())).setGeneration(1).build());
        return builder.build();
    }

    /** A project with two plain indices. */
    private static ProjectMetadata twoIndexProject(String name1, String name2) {
        return ProjectMetadata.builder(ProjectId.DEFAULT).put(plainMeta(name1), false).put(plainMeta(name2), false).build();
    }

    /** Plain source doc — a simple JSON object. */
    private static BytesReference doc(int i) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder().startObject().field("val", i).endObject();
        return BytesReference.bytes(b);
    }

    /** Inline-source IndexRequest targeting the given index. */
    private static IndexRequest indexRequest(String index, BytesReference source) {
        return new IndexRequest(index).source(source, XContentType.JSON);
    }

    // ---- eligibility tests ----

    public void testEmptyBulkIsNotEligible() {
        assertFalse(BulkBatchEncoders.isBulkBatchEligible(new BulkRequest()));
    }

    public void testSimulatedBulkIsNotEligible() throws Exception {
        SimulateBulkRequest req = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of(), null);
        req.add(indexRequest(INDEX, doc(0)));
        assertFalse(BulkBatchEncoders.isBulkBatchEligible(req));
    }

    public void testBulkWithDeleteIsNotEligible() throws Exception {
        BulkRequest req = new BulkRequest();
        req.add(indexRequest(INDEX, doc(0)));
        req.add(new DeleteRequest(INDEX).id("d1"));
        assertFalse(BulkBatchEncoders.isBulkBatchEligible(req));
    }

    public void testBulkWithUpdateIsNotEligible() throws Exception {
        BulkRequest req = new BulkRequest();
        req.add(indexRequest(INDEX, doc(0)));
        req.add(new UpdateRequest(INDEX, "u1").doc(Map.of("v", 1)));
        assertFalse(BulkBatchEncoders.isBulkBatchEligible(req));
    }

    public void testItemAlreadyHavingSourceRowIsNotEligible() throws Exception {
        // Once setSourceRow is called, isItemBatchEligible must return false.
        IndexRequest request = indexRequest(INDEX, doc(0));
        // Build a tiny batch just to get a SourceBatch to attach.
        BulkRequest eligible = new BulkRequest();
        eligible.add(indexRequest(INDEX, doc(0)));
        ProjectMetadata project = singleIndexProject(INDEX);
        BatchRouterSet router = BulkBatchEncoders.encode(eligible, project, RESOLVER);
        assertNotNull(router);
        // The request in `eligible` now has hasSourceRow == true.
        IndexRequest encoded = (IndexRequest) eligible.requests().get(0);
        assertFalse(BulkBatchEncoders.isItemBatchEligible(encoded));
    }

    // ---- encode() tests ----

    public void testEncodeSingleIndexReturnsNonNullWithCorrectRowCount() throws Exception {
        int n = between(1, 20);
        BulkRequest req = new BulkRequest();
        List<BytesReference> sources = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            BytesReference src = doc(i);
            sources.add(src);
            req.add(indexRequest(INDEX, src));
        }
        ProjectMetadata project = singleIndexProject(INDEX);
        BatchRouterSet router = BulkBatchEncoders.encode(req, project, RESOLVER);
        assertThat(router, notNullValue());
        // Every IndexRequest must now carry a source-row reference.
        for (int i = 0; i < n; i++) {
            IndexRequest ir = (IndexRequest) req.requests().get(i);
            assertTrue("item " + i + " should have a source row", ir.indexSource().hasSourceRow());
            assertThat("item " + i + " should be row " + i, ir.indexSource().rowIndex(), equalTo(i));
        }
    }

    public void testEncodeTwoAbstractionsProducesSeparateRowSequences() throws Exception {
        String idx1 = "index-a";
        String idx2 = "index-b";
        int n1 = between(1, 10);
        int n2 = between(1, 10);
        BulkRequest req = new BulkRequest();
        for (int i = 0; i < n1; i++) {
            req.add(indexRequest(idx1, doc(i)));
        }
        for (int i = 0; i < n2; i++) {
            req.add(indexRequest(idx2, doc(100 + i)));
        }
        ProjectMetadata project = twoIndexProject(idx1, idx2);
        BatchRouterSet router = BulkBatchEncoders.encode(req, project, RESOLVER);
        assertThat(router, notNullValue());

        // idx1 items: rows 0..n1-1; idx2 items: rows 0..n2-1 (separate batches).
        for (int i = 0; i < n1; i++) {
            IndexRequest ir = (IndexRequest) req.requests().get(i);
            assertTrue(ir.indexSource().hasSourceRow());
            assertThat("idx1 item " + i + " row", ir.indexSource().rowIndex(), equalTo(i));
        }
        for (int i = 0; i < n2; i++) {
            IndexRequest ir = (IndexRequest) req.requests().get(n1 + i);
            assertTrue(ir.indexSource().hasSourceRow());
            assertThat("idx2 item " + i + " row", ir.indexSource().rowIndex(), equalTo(i));
        }
        // Both abstractions must produce separately sequenced rows starting from 0 — the best
        // observable signal that they ended up in different batches without accessing private fields.
    }

    public void testEncodeDataStreamKeysToDataStreamName() throws Exception {
        // Data stream writes use CREATE op type; the resolver only expands data streams for CREATE.
        BulkRequest req = new BulkRequest();
        req.add(new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE).source(doc(0), XContentType.JSON));
        req.add(new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE).source(doc(1), XContentType.JSON));
        ProjectMetadata project = projectWithDataStream();
        BatchRouterSet router = BulkBatchEncoders.encode(req, project, RESOLVER);
        assertThat(router, notNullValue());
        for (int i = 0; i < 2; i++) {
            IndexRequest ir = (IndexRequest) req.requests().get(i);
            assertTrue("item " + i + " should have source row", ir.indexSource().hasSourceRow());
            assertThat("item " + i + " row", ir.indexSource().rowIndex(), equalTo(i));
        }
    }

    public void testEncodeDirectToBackingIndexReturnsNull() throws Exception {
        // A direct write to a .ds-... backing index (with CREATE op, so the resolver sees it as a
        // CONCRETE_INDEX with a parent data stream) must abort and return null.
        ProjectMetadata project = projectWithDataStream();
        String backingName = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1, EPOCH_MILLIS);
        BytesReference src = doc(0);

        BulkRequest req = new BulkRequest();
        // Backing index write uses CREATE; use CREATE here so the resolver sees the full metadata.
        req.add(new IndexRequest(backingName).opType(DocWriteRequest.OpType.CREATE).source(src, XContentType.JSON));
        BatchRouterSet router = BulkBatchEncoders.encode(req, project, RESOLVER);
        assertThat("direct write to backing index should return null", router, nullValue());

        // Inline bytes must still be intact on the request (deferred-attachment invariant).
        IndexRequest ir = (IndexRequest) req.requests().get(0);
        assertFalse("inline bytes must not have been destroyed on null return", ir.indexSource().hasSourceRow());
    }

    public void testEncodeFailurePreservesInlineBytes() throws Exception {
        // Verify the deferred-attachment invariant on a parse failure. We use a request with a
        // content type mismatch to trigger an IOException in EscfEncoder.addDocument.
        BulkRequest req = new BulkRequest();
        // First item: valid JSON.
        req.add(indexRequest(INDEX, doc(0)));
        // Second item: content type says JSON but bytes are not valid JSON → encoder will throw.
        IndexRequest badRequest = new IndexRequest(INDEX).source("{not valid json".getBytes(), XContentType.JSON);
        req.add(badRequest);

        BytesReference firstSrc = ((IndexRequest) req.requests().get(0)).indexSource().bytes();
        BytesReference secondSrc = badRequest.indexSource().bytes();

        ProjectMetadata project = singleIndexProject(INDEX);
        BatchRouterSet router = BulkBatchEncoders.encode(req, project, RESOLVER);
        // encode() should return null due to the parse failure.
        assertThat(router, nullValue());

        // Neither request should have been modified — inline bytes must survive.
        IndexRequest first = (IndexRequest) req.requests().get(0);
        assertFalse("first item must not have sourceRow on failure", first.indexSource().hasSourceRow());
        assertSame("first item bytes must be unchanged on failure", firstSrc, first.indexSource().bytes());

        assertFalse("bad item must not have sourceRow on failure", badRequest.indexSource().hasSourceRow());
        assertSame("bad item bytes must be unchanged on failure", secondSrc, badRequest.indexSource().bytes());
    }

    public void testEncodeReturnsNullForIneligibleBulk() throws Exception {
        // A bulk with a delete is ineligible; encode() must short-circuit and return null.
        BulkRequest req = new BulkRequest();
        req.add(indexRequest(INDEX, doc(0)));
        req.add(new DeleteRequest(INDEX).id("d1"));
        ProjectMetadata project = singleIndexProject(INDEX);
        assertThat(BulkBatchEncoders.encode(req, project, RESOLVER), nullValue());
    }

    // ---- alias tests ----

    public void testAliasOverPlainIndexKeysToWriteIndexName() throws Exception {
        String alias = "my-alias";
        String writeName = "write-index";
        IndexMetadata writeMeta = IndexMetadata.builder(writeName)
            .settings(indexSettings(writeName).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .putAlias(org.elasticsearch.cluster.metadata.AliasMetadata.builder(alias).writeIndex(true).build())
            .build();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(writeMeta, false).build();

        BulkRequest req = new BulkRequest();
        req.add(indexRequest(alias, doc(0)));
        BatchRouterSet router = BulkBatchEncoders.encode(req, project, RESOLVER);
        assertThat(router, notNullValue());
        IndexRequest ir = (IndexRequest) req.requests().get(0);
        assertTrue(ir.indexSource().hasSourceRow());
    }

    // ---- eligibility delegation tests ----

    public void testEncodeRespectsItemEligibilityForSingleItem() throws Exception {
        // A single valid IndexRequest with inline source → eligible.
        BulkRequest req = new BulkRequest();
        req.add(indexRequest(INDEX, doc(0)));
        assertTrue(BulkBatchEncoders.isItemBatchEligible((IndexRequest) req.requests().get(0)));
    }

    public void testItemWithoutSourceIsNotEligible() {
        // An IndexRequest with no source (e.g. from a pipeline) is not eligible.
        IndexRequest req = new IndexRequest(INDEX).id("x");
        // No source set → hasSource() == false.
        assertFalse(BulkBatchEncoders.isItemBatchEligible(req));
    }
}
