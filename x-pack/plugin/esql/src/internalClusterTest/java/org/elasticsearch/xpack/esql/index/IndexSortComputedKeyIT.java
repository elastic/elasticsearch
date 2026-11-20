/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * An index sorted with {@code index.sort} on a key computed by an ES|QL function, which is never stored.
 */
public class IndexSortComputedKeyIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "computed_sort";
    private static final long HOUR = 3_600_000L;
    private static final long START = 1_767_225_600_000L; // 2026-01-01T00:00:00Z

    public void testIndexSortedOnComputedKey() throws IOException {
        createIndex("DATE_TRUNC(1 hour, @timestamp) DESC, host.name");
        int numDocs = 0;
        int batches = between(2, 5);
        for (int b = 0; b < batches; b++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int docs = between(50, 300);
            for (int i = 0; i < docs; i++) {
                long timestamp = START + randomLongBetween(0, 5 * HOUR);
                bulk.add(prepareIndex(INDEX).setSource(Map.of("@timestamp", timestamp, "host.name", "host-" + between(0, 9))));
            }
            numDocs += docs;
            assertNoFailures(bulk.get());
        }
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
        refresh(INDEX);

        IndexShard shard = null;
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(INDEX));
            if (indexService != null) {
                shard = indexService.getShard(0);
            }
        }
        assertNotNull(shard);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                SortField[] sort = leaf.reader().getMetaData().sort().getSort();
                assertThat(sort[0], instanceOf(EsqlSortField.class));
                assertThat(sort[0].getField(), equalTo("DATE_TRUNC(1 hour, @timestamp)"));
                assertThat(sort[1].getField(), equalTo("host.name"));
                // The hour is computed from @timestamp, not stored in a field of its own
                Set<String> fields = new HashSet<>();
                for (FieldInfo fi : leaf.reader().getFieldInfos()) {
                    fields.add(fi.name);
                }
                assertThat(fields.stream().filter(f -> f.contains("DATE_TRUNC")).toList(), equalTo(List.of()));
            }
        }

        // The segments API reports the computed key by its text
        var segments = indicesAdmin().prepareSegments(INDEX).get();
        assertNoFailures(segments);
        for (var shardSegments : segments.getIndices().get(INDEX).getShards().get(0)) {
            for (var segment : shardSegments.getSegments()) {
                assertThat(segment.getSegmentSort().getSort()[0].getField(), equalTo("DATE_TRUNC(1 hour, @timestamp)"));
            }
        }

        // Documents come back in index order: hour descending, then host name ascending
        int expected = numDocs;
        assertResponse(prepareSearch(INDEX).addSort("_doc", SortOrder.ASC).setSize(numDocs), response -> {
            List<SearchHit> hits = List.of(response.getHits().getHits());
            assertThat(hits.size(), equalTo(expected));
            long previousHour = Long.MAX_VALUE;
            String previousHost = "";
            for (SearchHit hit : hits) {
                Map<String, Object> source = hit.getSourceAsMap();
                long hour = Math.floorDiv(((Number) source.get("@timestamp")).longValue(), HOUR) * HOUR;
                String host = (String) source.get("host.name");
                assertThat(hour, lessThanOrEqualTo(previousHour));
                if (hour == previousHour) {
                    assertThat(host + " after " + previousHost, previousHost.compareTo(host) <= 0, equalTo(true));
                }
                previousHour = hour;
                previousHost = host;
            }
        });
    }

    public void testRestoreCannotChangeOrRemoveIndexSort() {
        assertAcked(
            prepareCreate("test-idx").setMapping("host.name", "type=keyword", "@timestamp", "type=date")
                .setSettings(Settings.builder().put(indexSettings()).put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC, host.name"))
        );
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setIndices("test-idx")
            .setWaitForCompletion(true)
            .get();
        cluster().wipeIndices("test-idx");

        var modifyError = expectThrows(
            SnapshotRestoreException.class,
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
                .setIndexSettings(Settings.builder().put("index.sort", "host.name"))
                .setWaitForCompletion(true)
        );
        assertThat(modifyError.getMessage(), containsString("cannot modify setting [index.sort] on restore"));

        var removeError = expectThrows(
            SnapshotRestoreException.class,
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
                .setIgnoreIndexSettings("index.sort")
                .setWaitForCompletion(true)
        );
        assertThat(removeError.getMessage(), containsString("cannot remove setting [index.sort] on restore"));
    }

    public void testRejectsInvalidKeysAtCreation() {
        assertCreateFails("NOW() DESC", "uses [NOW()], which isn't guaranteed to compute the same values on every node and version");
        assertCreateFails("MAX(@timestamp)", "uses [MAX(@timestamp)], which can't be computed for each document");
        assertCreateFails("DATE_TRUNC(1 hour, missing)", "references unknown field [missing]");
        assertCreateFails("COALESCE(host.name, \"a\")", "returns type [keyword], which isn't supported yet");
        assertCreateFails("DATE_TRUNC(1 hour, host.name)", "second argument of [DATE_TRUNC(1 hour, host.name)]");
    }

    public void testRejectsInvalidKeysInTemplates() {
        Exception e = expectThrows(InvalidIndexTemplateException.class, () -> putTemplate("NOW()"));
        assertThat(
            e.getMessage(),
            containsString(
                "index sort key [NOW()] uses [NOW()], which isn't guaranteed to compute the same values on every node and version"
            )
        );
        assertAcked(putTemplate("DATE_TRUNC(1 hour, @timestamp) DESC"));
    }

    public void testRejectsFieldsItCannotRead() {
        assertCreateFails(
            "DATE_TRUNC(1 hour, ts)",
            "{\"properties\":{\"ts\":{\"type\":\"date\",\"doc_values\":false}}}",
            "needs doc values on field [ts]"
        );
        assertCreateFails(
            "DATE_TRUNC(1 hour, ts)",
            "{\"properties\":{\"t\":{\"type\":\"date\"},\"ts\":{\"type\":\"alias\",\"path\":\"t\"}}}",
            "can't use alias [ts]"
        );
        assertCreateFails(
            "DATE_TRUNC(1 hour, ts)",
            "{\"runtime\":{\"ts\":{\"type\":\"date\"}}}",
            "runtime field [ts] shadows an index sort field"
        );
        assertCreateFails("LENGTH(ts)", "{\"properties\":{\"ts\":{\"type\":\"text\"}}}", "needs doc values on field [ts]");
        assertCreateFails("DATE_TRUNC(1 hour, n)", "{\"properties\":{\"n\":{\"type\":\"long\"}}}", "index sort key");
        assertCreateFails(
            "DATE_TRUNC(1 hour, n.ts)",
            "{\"properties\":{\"n\":{\"type\":\"nested\",\"properties\":{\"ts\":{\"type\":\"date\"}}}}}",
            "under nested object [n]"
        );
    }

    /** A runtime field can't later shadow a field that a computed key reads, or the index sort can't be built any more. */
    public void testRejectsRuntimeFieldShadowingKeyField() {
        createIndex("DATE_TRUNC(1 hour, @timestamp) DESC, host.name");
        Exception e = expectThrows(
            Exception.class,
            () -> indicesAdmin().preparePutMapping(INDEX).setSource("{\"runtime\":{\"@timestamp\":{\"type\":\"date\"}}}").get()
        );
        assertThat(e.getMessage(), containsString("shadows an index sort field"));
    }

    /** Nested documents keep their parents in key order. */
    public void testNestedDocuments() throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(1, 0).put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC"))
                .setMapping("{\"properties\":{\"@timestamp\":{\"type\":\"date\",\"format\":\"epoch_millis\"},\"n\":{\"type\":\"nested\"}}}")
        );
        int numDocs = indexRandomDocs(between(2, 4), true, false);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
        refresh(INDEX);
        assertHourOrder(INDEX, numDocs);
    }

    /** Closing and opening the index rebuilds the sort, which keeps merging old and new segments. */
    public void testReopen() throws IOException {
        createIndex("DATE_TRUNC(1 hour, @timestamp) DESC, host.name");
        int numDocs = indexRandomDocs(between(1, 3), false, false);
        assertAcked(indicesAdmin().prepareClose(INDEX));
        assertAcked(indicesAdmin().prepareOpen(INDEX));
        ensureGreen(INDEX);
        numDocs += indexRandomDocs(between(1, 3), false, false);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
        refresh(INDEX);
        assertHourOrder(INDEX, numDocs);
    }

    /** Shrink and split copy the sorted segments into an index with the same sort. */
    public void testResize() throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    indexSettings(2, 0).put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC, host.name")
                        // Shrinking needs every shard on the same node
                        .put("index.routing.allocation.require._name", internalCluster().getRandomDataNodeName())
                        .put("index.number_of_routing_shards", 8)
                )
                .setMapping("@timestamp", "type=date,format=epoch_millis", "host.name", "type=keyword")
        );
        int numDocs = indexRandomDocs(between(1, 3), false, false);
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), INDEX);
        ResizeType type = randomFrom(ResizeType.SHRINK, ResizeType.SPLIT);
        String target = "computed_sort_" + type.name().toLowerCase(Locale.ROOT);
        assertAcked(
            executeResize(
                type,
                INDEX,
                target,
                indexSettings(type == ResizeType.SHRINK ? 1 : 4, 0).putNull("index.blocks.write")
                    .putNull("index.routing.allocation.require._name")
            )
        );
        ensureGreen(target);
        assertNoFailures(indicesAdmin().prepareForceMerge(target).setMaxNumSegments(1).get());
        refresh(target);
        assertHourOrder(target, numDocs);
    }

    public void testLogsdb() throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    indexSettings(1, 0).put("index.mode", "logsdb").put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC, host.name")
                )
                .setMapping("@timestamp", "type=date,format=epoch_millis", "host.name", "type=keyword")
        );
        int numDocs = indexRandomDocs(between(1, 3), false, true);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
        refresh(INDEX);
        assertHourOrder(INDEX, numDocs);
    }

    /**
     * Indexes batches of documents, some without a timestamp and some with several, which have no key, unless
     * {@code singleTimestamp} as logsdb requires.
     */
    private int indexRandomDocs(int batches, boolean nested, boolean singleTimestamp) {
        int numDocs = 0;
        for (int b = 0; b < batches; b++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int docs = between(50, 300);
            for (int i = 0; i < docs; i++) {
                Map<String, Object> source = new HashMap<>();
                source.put("host.name", "host-" + between(0, 9));
                switch (singleTimestamp ? 9 : between(0, 9)) {
                    case 0 -> {
                    }
                    case 1 -> source.put(
                        "@timestamp",
                        List.of(START + randomLongBetween(0, 5 * HOUR), START + randomLongBetween(0, 5 * HOUR))
                    );
                    default -> source.put("@timestamp", START + randomLongBetween(0, 5 * HOUR));
                }
                if (nested) {
                    source.put("n", List.of(Map.of("v", i), Map.of("v", i + 1)));
                }
                bulk.add(prepareIndex(INDEX).setSource(source));
            }
            numDocs += docs;
            assertNoFailures(bulk.get());
        }
        return numDocs;
    }

    /** Documents come back in index order: hour descending with documents without a single timestamp first, as DESC puts nulls first. */
    private void assertHourOrder(String index, int numDocs) {
        assertResponse(prepareSearch(index).addSort("_doc", SortOrder.ASC).setSize(numDocs), response -> {
            assertThat(response.getHits().getHits().length, equalTo(numDocs));
        });
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            if (indexService == null) {
                continue;
            }
            for (IndexShard shard : indexService) {
                try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                    for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                        var timestamps = leaf.reader().getSortedNumericDocValues("@timestamp");
                        var parents = leaf.reader().getFieldInfos().getParentField();
                        var parentDocs = parents == null ? null : leaf.reader().getNumericDocValues(parents);
                        long previous = Long.MAX_VALUE;
                        for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                            if (parentDocs != null && parentDocs.advanceExact(doc) == false) {
                                continue;
                            }
                            long hour = Long.MAX_VALUE;
                            if (timestamps != null && timestamps.advanceExact(doc) && timestamps.docValueCount() == 1) {
                                hour = Math.floorDiv(timestamps.nextValue(), HOUR) * HOUR;
                            }
                            assertThat("doc " + doc, hour, lessThanOrEqualTo(previous));
                            previous = hour;
                        }
                    }
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }

    private void assertCreateFails(String sort, String mapping, String message) {
        Exception e = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(1, 0).put("index.sort", sort)).setMapping(mapping).get()
        );
        assertThat(e.getMessage(), containsString(message));
    }

    private void createIndex(String sort) {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.sort", sort))
                .setMapping("@timestamp", "type=date,format=epoch_millis", "host.name", "type=keyword")
        );
    }

    private void assertCreateFails(String sort, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createIndex(sort));
        assertThat(e.getMessage(), containsString(message));
    }

    private AcknowledgedResponse putTemplate(String sort) {
        var request = new TransportPutComposableIndexTemplateAction.Request("computed_sort_template");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(new ArrayList<>(List.of("computed-sort-*")))
                .template(Template.builder().settings(Settings.builder().put("index.sort", sort)))
                .build()
        );
        return client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }
}
