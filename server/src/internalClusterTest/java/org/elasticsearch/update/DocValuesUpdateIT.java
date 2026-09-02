/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.update;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DocValuesUpdateIT extends ESIntegTestCase {

    private static final String MAPPING = """
        {
          "properties": {
            "status": { "type": "keyword", "index": false, "doc_values": { "updatable": true } },
            "count":  { "type": "long",    "index": false, "doc_values": { "updatable": true } },
            "name":   { "type": "keyword" }
          }
        }
        """;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Exercise both the direct and pre-resolved bulk-update paths; the in-place fast path must trigger on either.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("indices.pre_resolve_bulk_updates", randomBoolean())
            .build();
    }

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        // Columnar mode requires DOC_VALUES_ONLY for seq_no; drop the randomly-chosen value so it doesn't conflict with the mode default.
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    public void testBulkUpdateOfUpdatableFieldsAppliesInPlace() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    // updatable fields need sequence numbers, which columnar disables by default
                    .put("index.disable_sequence_numbers", false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
            ).setMapping(MAPPING)
        );
        ensureGreen("idx");

        DocWriteResponse indexResponse = prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(1L));

        // Update only the updatable fields; this must be applied in place, not by reindexing the document. The write waits for the
        // replica so the replica path is exercised too.
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42)));
        BulkResponse bulkResponse = client().bulk(bulk).actionGet();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
        assertThat(bulkResponse.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        // The response reports the document's unchanged seq_no, not the update operation's internal one.
        assertThat(bulkResponse.getItems()[0].getResponse().getSeqNo(), equalTo(indexResponse.getSeqNo()));

        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertTrue(get.isExists());
        // An in-place doc-values update does not bump the document's version; a reindex fallback would have made it 2.
        assertThat("expected an in-place update, not a reindex", get.getVersion(), equalTo(1L));
        Map<String, Object> source = get.getSourceAsMap();
        assertThat(source.get("status"), equalTo("active"));
        assertThat(((Number) source.get("count")).longValue(), equalTo(42L));
        // The untouched field is preserved.
        assertThat(source.get("name"), equalTo("widget"));
    }

    public void testUpdateTouchingNonUpdatableFieldFallsBackToReindex() throws Exception {
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
            ).setMapping(MAPPING)
        );
        ensureGreen("idx");
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // "name" is not updatable, so the whole update must fall back to a reindex, which bumps the version.
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "name", "gadget")));
        BulkResponse bulkResponse = client().bulk(bulk).actionGet();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());

        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat("expected a reindex fallback", get.getVersion(), equalTo(2L));
        assertThat(get.getSourceAsMap().get("status"), equalTo("active"));
        assertThat(get.getSourceAsMap().get("name"), equalTo("gadget"));
    }

    public void testColumnarStoredSourceOverlaysDocValuesUpdates() throws Exception {
        // In columnar_stored source mode _source is a whole-document blob written at index time. An in-place update mutates only the
        // doc-values column, so _source is reconstructed by overlaying the updated fields' current doc values back on top. This also
        // covers the floating-point decode path (double) in addition to keyword and long.
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("""
                {
                  "properties": {
                    "status": { "type": "keyword",    "index": false, "doc_values": { "updatable": true } },
                    "count":  { "type": "long",       "index": false, "doc_values": { "updatable": true } },
                    "ratio":  { "type": "double",     "index": false, "doc_values": { "updatable": true } },
                    "score":  { "type": "float",      "index": false, "doc_values": { "updatable": true } },
                    "temp":   { "type": "half_float", "index": false, "doc_values": { "updatable": true } },
                    "name":   { "type": "keyword" }
                  }
                }
                """)
        );
        ensureGreen("idx");

        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "ratio", 0.5, "score", 0.25, "temp", 0.5, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42, "ratio", 2.5, "score", 4.25, "temp", 8.5)));
        BulkResponse bulkResponse = client().bulk(bulk).actionGet();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
        assertThat(bulkResponse.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        // GET returns the overlaid values, including every floating-point decode path, and the untouched field from the stored blob
        // survives.
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat("expected an in-place update, not a reindex", get.getVersion(), equalTo(1L));
        assertOverlaidSource(get.getSourceAsMap());

        // The same overlaid _source is returned through the search fetch phase.
        assertResponse(
            prepareSearch("idx").setQuery(QueryBuilders.matchAllQuery()),
            response -> assertOverlaidSource(response.getHits().getAt(0).getSourceAsMap())
        );

        // A force-merge re-materializes the corrected source and resets the fields' doc-values generation, so the read-time overlay is
        // skipped afterwards. The source must still be correct, which proves the merge folded the update into the stored blob.
        assertNoFailures(indicesAdmin().prepareForceMerge("idx").setMaxNumSegments(1).get());
        GetResponse afterMerge = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(afterMerge.getVersion(), equalTo(1L));
        assertOverlaidSource(afterMerge.getSourceAsMap());
        assertResponse(
            prepareSearch("idx").setQuery(QueryBuilders.matchAllQuery()),
            response -> assertOverlaidSource(response.getHits().getAt(0).getSourceAsMap())
        );
    }

    public void testInPlaceUpdateOnLogsdbColumnar() throws Exception {
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping(MAPPING)
        );
        ensureGreen("idx");
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget", "@timestamp", "2026-01-01T00:00:00Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        UpdateResponse update = client().prepareUpdate("idx", "1")
            .setDoc(Map.of("status", "active", "count", 42))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(update.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        assertThat("expected an in-place update, not a reindex", update.getVersion(), equalTo(1L));
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(get.getSourceAsMap().get("status"), equalTo("active"));
        assertThat(((Number) get.getSourceAsMap().get("count")).longValue(), equalTo(42L));
        assertThat(get.getSourceAsMap().get("name"), equalTo("widget"));
    }

    private static void assertOverlaidSource(Map<String, Object> source) {
        assertThat(source.get("status"), equalTo("active"));
        assertThat(((Number) source.get("count")).longValue(), equalTo(42L));
        assertThat(((Number) source.get("ratio")).doubleValue(), equalTo(2.5));
        assertThat(((Number) source.get("score")).floatValue(), equalTo(4.25f));
        assertThat(((Number) source.get("temp")).floatValue(), equalTo(8.5f));
        assertThat(source.get("name"), equalTo("widget"));
    }

    private void createColumnarIndex(int replicas) {
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", replicas)
            ).setMapping(MAPPING)
        );
        ensureGreen("idx");
    }

    /**
     * An in-place doc-values update is not visible to a realtime get until a refresh — it becomes visible at the same time as it does to
     * search, because the update leaves the document's translog index operation (which realtime get reads) untouched. This test pins that
     * behaviour so a change to it is deliberate.
     */
    public void testRealtimeGetIsStaleUntilRefresh() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1").setSource("status", "new", "count", 1, "name", "widget").get();
        refresh("idx");

        BulkRequest bulk = new BulkRequest(); // note: no immediate refresh
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42)));
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        // Before a refresh the realtime get still returns the pre-update value.
        GetResponse realtime = client().prepareGet("idx", "1").setRealtime(true).get();
        assertThat(realtime.getSourceAsMap().get("status"), equalTo("new"));
        assertThat(((Number) realtime.getSourceAsMap().get("count")).longValue(), equalTo(1L));

        // After a refresh the update is visible.
        refresh("idx");
        GetResponse afterRefresh = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(afterRefresh.getSourceAsMap().get("status"), equalTo("active"));
        assertThat(((Number) afterRefresh.getSourceAsMap().get("count")).longValue(), equalTo(42L));
    }

    /**
     * Requesting {@code _source} back disables the fast path and takes the read-modify path, but the update must still apply in place:
     * the document version stays unchanged where a reindex would have bumped it.
     */
    public void testUpdateRequestingSourceStillAppliesInPlace() throws Exception {
        createColumnarIndex(0);
        DocWriteResponse indexed = prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        UpdateResponse update = client().prepareUpdate("idx", "1")
            .setDoc(Map.of("status", "active", "count", 42))
            .setFetchSource(true)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(update.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        // Still an in-place update: the document version does not change, a reindex would have bumped it to 2.
        assertThat(update.getVersion(), equalTo(1L));
        assertThat(update.getSeqNo(), equalTo(indexed.getSeqNo()));

        // The response echoes the merged source, rebuilt from the update map.
        assertThat(update.getGetResult(), notNullValue());
        assertThat(update.getGetResult().sourceAsMap().get("status"), equalTo("active"));
        assertThat(((Number) update.getGetResult().sourceAsMap().get("count")).longValue(), equalTo(42L));
        assertThat(update.getGetResult().sourceAsMap().get("name"), equalTo("widget"));

        // The applied values are visible after a refresh.
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(get.getVersion(), equalTo(1L));
        assertThat(get.getSourceAsMap().get("status"), equalTo("active"));
        assertThat(((Number) get.getSourceAsMap().get("count")).longValue(), equalTo(42L));
        assertThat(get.getSourceAsMap().get("name"), equalTo("widget"));
    }

    public void testSearchAndAggregationSeeUpdatedValues() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42)));
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        // The updated column is visible to search (a range query on the numeric) and to a terms aggregation on the keyword — proving the
        // value was written to the doc-values column, not merely reflected in _source.
        assertResponse(prepareSearch("idx").setQuery(QueryBuilders.rangeQuery("count").gte(40)), response -> assertHitCount(response, 1));
        assertResponse(prepareSearch("idx").setSize(0).addAggregation(AggregationBuilders.terms("byStatus").field("status")), response -> {
            Terms terms = response.getAggregations().get("byStatus");
            assertThat(terms.getBuckets().size(), equalTo(1));
            assertThat(terms.getBuckets().get(0).getKeyAsString(), equalTo("active"));
        });
        // The stale value is gone from search.
        assertResponse(prepareSearch("idx").setQuery(QueryBuilders.termQuery("status", "new")), response -> assertHitCount(response, 0));
    }

    public void testRepeatedUpdatesLastWins() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1")
            .setSource("status", "s0", "count", 0, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        for (int i = 1; i <= 5; i++) {
            BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "s" + i, "count", i)));
            assertFalse(client().bulk(bulk).actionGet().hasFailures());
        }

        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(get.getSourceAsMap().get("status"), equalTo("s5"));
        assertThat(((Number) get.getSourceAsMap().get("count")).longValue(), equalTo(5L));
        // Still an in-place update every time, so the version never moved.
        assertThat(get.getVersion(), equalTo(1L));
    }

    public void testMixedBulkOfUpdatesIndexesAndDeletes() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1").setSource("status", "new", "count", 1, "name", "a").get();
        prepareIndex("idx").setId("2").setSource("status", "new", "count", 2, "name", "b").get();
        refresh("idx");

        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42))); // doc-values update
        bulk.add(new IndexRequest("idx").id("3").source("status", "new", "count", 3, "name", "c")); // normal index
        bulk.add(new DeleteRequest("idx", "2")); // delete
        BulkResponse response = client().bulk(bulk).actionGet();
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        assertThat(
            ((Number) client().prepareGet("idx", "1").setRealtime(false).get().getSourceAsMap().get("count")).longValue(),
            equalTo(42L)
        );
        assertFalse(client().prepareGet("idx", "2").setRealtime(false).get().isExists());
        assertTrue(client().prepareGet("idx", "3").setRealtime(false).get().isExists());
    }

    public void testAllSupportedTypesUpdateInPlace() throws Exception {
        // Every updatable type, including all three floating-point types (each with its own sortable-long decode) and the narrow
        // integer types. The floating-point values chosen are exactly representable, including in 16-bit half_float.
        String mapping = """
            {
              "properties": {
                "kw": { "type": "keyword",    "index": false, "doc_values": { "updatable": true } },
                "l":  { "type": "long",       "index": false, "doc_values": { "updatable": true } },
                "i":  { "type": "integer",    "index": false, "doc_values": { "updatable": true } },
                "s":  { "type": "short",      "index": false, "doc_values": { "updatable": true } },
                "b":  { "type": "byte",       "index": false, "doc_values": { "updatable": true } },
                "d":  { "type": "double",     "index": false, "doc_values": { "updatable": true } },
                "f":  { "type": "float",      "index": false, "doc_values": { "updatable": true } },
                "hf": { "type": "half_float", "index": false, "doc_values": { "updatable": true } }
              }
            }
            """;
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
            ).setMapping(mapping)
        );
        ensureGreen("idx");
        prepareIndex("idx").setId("1")
            .setSource("kw", "a", "l", 1, "i", 2, "s", 3, "b", 4, "d", 1.5, "f", 2.5, "hf", 1.5)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("kw", "b", "l", 10, "i", 20, "s", 30, "b", 40, "d", 9.5, "f", 8.5, "hf", 8.5)));
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat("in place, version unchanged", get.getVersion(), equalTo(1L));
        Map<String, Object> source = get.getSourceAsMap();
        assertThat(source.get("kw"), equalTo("b"));
        assertThat(((Number) source.get("l")).longValue(), equalTo(10L));
        assertThat(((Number) source.get("i")).intValue(), equalTo(20));
        assertThat(((Number) source.get("s")).intValue(), equalTo(30));
        assertThat(((Number) source.get("b")).intValue(), equalTo(40));
        assertThat(((Number) source.get("d")).doubleValue(), equalTo(9.5));
        assertThat(((Number) source.get("f")).floatValue(), equalTo(8.5f));
        assertThat(((Number) source.get("hf")).floatValue(), equalTo(8.5f));
        // The floating-point columns are searchable at their new values, proving the sortable-long encoding matched indexing.
        assertResponse(prepareSearch("idx").setQuery(QueryBuilders.rangeQuery("d").gte(9.0)), response -> assertHitCount(response, 1));
        assertResponse(prepareSearch("idx").setQuery(QueryBuilders.rangeQuery("f").gte(8.0)), response -> assertHitCount(response, 1));
        assertResponse(prepareSearch("idx").setQuery(QueryBuilders.rangeQuery("hf").gte(8.0)), response -> assertHitCount(response, 1));
    }

    public void testConditionalUpdateHonoursSeqNo() throws Exception {
        createColumnarIndex(0);
        DocWriteResponse indexed = prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // An update carrying if_seq_no cannot use the in-place path: an in-place update leaves the document's seq_no unchanged, so it is
        // invisible to seq_no CAS. It falls back to the read-modify-reindex update, which enforces the precondition.

        // A stale if_seq_no is rejected.
        Exception e = expectThrows(
            Exception.class,
            () -> client().update(
                new UpdateRequest("idx", "1").doc(Map.of("status", "active"))
                    .setIfSeqNo(indexed.getSeqNo() + 5)
                    .setIfPrimaryTerm(indexed.getPrimaryTerm())
            ).actionGet()
        );
        assertThat(e.getMessage(), containsString("version conflict"));

        // A matching if_seq_no succeeds via the reindex fallback, which bumps the version (an in-place update would have left it at 1).
        client().update(
            new UpdateRequest("idx", "1").doc(Map.of("status", "active"))
                .setIfSeqNo(indexed.getSeqNo())
                .setIfPrimaryTerm(indexed.getPrimaryTerm())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(get.getSourceAsMap().get("status"), equalTo("active"));
        assertThat("a conditional update falls back to reindex, bumping the version", get.getVersion(), equalTo(2L));
    }

    public void testInPlaceUpdateReturnsDocumentSeqNo() throws Exception {
        createColumnarIndex(0);
        DocWriteResponse indexed = prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // An in-place doc-values update does not change the document's seq_no, primary term or version. The response reports the
        // document's values (not the update operation's own seq_no, which is internal to replication), so a follow-up if_seq_no matches.
        UpdateResponse updated = client().update(
            new UpdateRequest("idx", "1").doc(Map.of("status", "active")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertThat(updated.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        assertThat("in-place update reports the document's unchanged seq_no", updated.getSeqNo(), equalTo(indexed.getSeqNo()));
        assertThat(updated.getPrimaryTerm(), equalTo(indexed.getPrimaryTerm()));
        assertThat(updated.getVersion(), equalTo(1L));

        // The reported seq_no actually addresses the document: a CAS on it is accepted rather than conflicting.
        client().update(
            new UpdateRequest("idx", "1").doc(Map.of("count", 7))
                .setIfSeqNo(updated.getSeqNo())
                .setIfPrimaryTerm(updated.getPrimaryTerm())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(((Number) get.getSourceAsMap().get("count")).longValue(), equalTo(7L));
    }

    public void testUpdateFieldAbsentInSomeDocuments() throws Exception {
        createColumnarIndex(0);
        // doc 1 has status; doc 2 omits it (nullable). The field exists in the index globally, so the update can set it on doc 2.
        prepareIndex("idx").setId("1").setSource("status", "new", "count", 1, "name", "a").get();
        prepareIndex("idx").setId("2").setSource("count", 2, "name", "b").get();
        refresh("idx");

        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "2").doc(Map.of("status", "active")));
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        assertThat(client().prepareGet("idx", "2").setRealtime(false).get().getSourceAsMap().get("status"), equalTo("active"));
        assertResponse(
            prepareSearch("idx").setSize(0).setQuery(QueryBuilders.termQuery("status", "active")),
            response -> assertHitCount(response, 1)
        );
    }

    public void testUpdatesSurvivePeerRecovery() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        // Start with no replica, index and update in place, then add a replica so it is built by peer recovery (file-based phase 1 copies
        // the updated doc-values generation, phase 2 replays any remaining ops). Reading the replica must return the updated values.
        createColumnarIndex(0);
        int docs = 20;
        for (int i = 0; i < docs; i++) {
            prepareIndex("idx").setId(Integer.toString(i)).setSource("status", "new", "count", i, "name", "n" + i).get();
        }
        refresh("idx");
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(new UpdateRequest("idx", Integer.toString(i)).doc(Map.of("status", "active", "count", i + 1000)));
        }
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        assertAcked(indicesAdmin().prepareUpdateSettings("idx").setSettings(Settings.builder().put("index.number_of_replicas", 1)));
        ensureGreen("idx");

        // Read specifically from the node holding the peer-recovered replica so its copy of the data is what is verified.
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        String replicaNodeId = state.routingTable().index("idx").shard(0).replicaShards().get(0).currentNodeId();
        String replicaPreference = "_only_nodes:" + replicaNodeId;
        assertResponse(
            prepareSearch("idx").setPreference(replicaPreference).setSize(0).setQuery(QueryBuilders.termQuery("status", "active")),
            response -> assertHitCount(response, docs)
        );
        assertResponse(
            prepareSearch("idx").setPreference(replicaPreference).setSize(0).setQuery(QueryBuilders.rangeQuery("count").gte(1000)),
            response -> assertHitCount(response, docs)
        );

        // The recovered replica agrees with the primary on the document metadata, not just the values: an in-place update leaves the
        // document's version, seq_no and primary term unchanged, and both copies must report the same ones after peer recovery.
        String primaryNodeId = state.routingTable().index("idx").shard(0).primaryShard().currentNodeId();
        GetResponse fromReplica = client().prepareGet("idx", "5").setRealtime(false).setPreference(replicaPreference).get();
        GetResponse fromPrimary = client().prepareGet("idx", "5").setRealtime(false).setPreference("_only_nodes:" + primaryNodeId).get();
        assertThat(fromReplica.getSourceAsMap().get("status"), equalTo("active"));
        assertThat("in-place update does not bump the version", fromReplica.getVersion(), equalTo(1L));
        assertThat(fromReplica.getSeqNo(), equalTo(fromPrimary.getSeqNo()));
        assertThat(fromReplica.getPrimaryTerm(), equalTo(fromPrimary.getPrimaryTerm()));
        assertThat(fromReplica.getVersion(), equalTo(fromPrimary.getVersion()));
    }

    public void testNullValueUpdateFallsBackToReindex() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        // A null value (field removal) must not take the in-place path; it falls back to a reindex, which bumps the version.
        Map<String, Object> partial = new HashMap<>();
        partial.put("status", null);
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(partial));
        BulkResponse resp = client().bulk(bulk).actionGet();
        assertFalse(resp.buildFailureMessage(), resp.hasFailures());
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat("null value falls back to reindex, bumping the version", get.getVersion(), equalTo(2L));
    }

    public void testMultiValuedUpdateValueDoesNotApplyInPlace() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        // A multi-valued value must never take the in-place path, which would flatten the array into a single doc-values value. It falls
        // back to the reindex path, where the single-valued updatable field rejects it — so the document is left untouched, not corrupted.
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", List.of("a", "b"))));
        BulkResponse resp = client().bulk(bulk).actionGet();
        assertTrue("a multi-valued update of a single-valued updatable field must be rejected, not applied", resp.hasFailures());
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(get.getSourceAsMap().get("status"), equalTo("new"));
        assertThat(get.getVersion(), equalTo(1L));
    }

    public void testHistoryDocIsInvisible() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active")));
        assertFalse(client().bulk(bulk).actionGet().hasFailures());
        // The born-soft-deleted history document that carries the __dv_update payload is invisible: it never leaks into _source, and a
        // match_all returns exactly the live document.
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertFalse("history-doc payload must not leak into _source", get.getSourceAsMap().containsKey("__dv_update"));
        assertResponse(prepareSearch("idx").setSize(0).setQuery(QueryBuilders.matchAllQuery()), response -> assertHitCount(response, 1));
    }

    public void testUpdatesSurviveForceMerge() throws Exception {
        createColumnarIndex(0);
        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42)));
        assertFalse(client().bulk(bulk).actionGet().hasFailures());
        // Force-merging to a single segment collapses the updated doc-values generation and expunges the soft-deleted history doc. The
        // updated values must survive and the live document count must stay 1.
        indicesAdmin().prepareForceMerge("idx").setMaxNumSegments(1).setFlush(true).get();
        refresh("idx");
        GetResponse get = client().prepareGet("idx", "1").setRealtime(false).get();
        assertThat(get.getSourceAsMap().get("status"), equalTo("active"));
        assertThat(((Number) get.getSourceAsMap().get("count")).longValue(), equalTo(42L));
        assertResponse(prepareSearch("idx").setSize(0).setQuery(QueryBuilders.matchAllQuery()), response -> assertHitCount(response, 1));
    }
}
