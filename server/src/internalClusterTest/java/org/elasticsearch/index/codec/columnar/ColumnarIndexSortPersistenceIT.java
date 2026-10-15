/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * A columnar index sorted by a multi-valued keyword keeps its sort through every path that rebuilds or reopens its segments from disk:
 * close and reopen, snapshot and restore, peer recovery to a replica, and shrink, split and clone. Each of these reads the index sort
 * back out of the segment metadata, where the keyword's binary sort field is serialized by its own provider, and resize additionally
 * requires the sort of the copied segments to equal the target's. A sort that failed to round-trip would either fail the operation or
 * leave segments, and the merges built on them, out of order without any error.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ColumnarIndexSortPersistenceIT extends ESIntegTestCase {

    private static final String INDEX = "sorted";

    private final Map<String, List<String>> live = new HashMap<>();
    private int nextId = 0;
    private boolean max;
    private boolean descending;
    private boolean missingFirst;
    private IndexMode mode;

    public void testColumnarSortSurvivesReopenRestoreRecoveryAndResize() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(IndexMode.COLUMNAR);
    }

    public void testLogsdbColumnarSortSurvivesReopenRestoreRecoveryAndResize() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(IndexMode.LOGSDB_COLUMNAR);
    }

    private void assertSortSurvivesReopenRestoreRecoveryAndResize(IndexMode indexMode) throws Exception {
        mode = indexMode;
        max = randomBoolean();
        descending = randomBoolean();
        missingFirst = randomBoolean();

        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    indexSettings(2, 0).put(IndexSettings.MODE.getKey(), mode)
                        .put("index.number_of_routing_shards", 4)
                        .put("index.sort.field", "kw")
                        .put("index.sort.mode", max ? "max" : "min")
                        .put("index.sort.order", descending ? "desc" : "asc")
                        .put("index.sort.missing", missingFirst ? "_first" : "_last")
                )
                .setMapping("""
                    {"properties":{"@timestamp":{"type":"date"},"kw":{"type":"keyword"}}}""")
        );
        ensureGreen(INDEX);
        for (int batch = between(2, 4); batch > 0; batch--) {
            indexBatch();
        }
        deleteSome();
        assertSortedAfterMerge(INDEX, null);

        // Close and reopen: the shards open their segments from disk again.
        assertAcked(indicesAdmin().prepareClose(INDEX));
        assertAcked(indicesAdmin().prepareOpen(INDEX));
        ensureGreen(INDEX);
        indexBatch();
        assertSortedAfterMerge(INDEX, null);

        // Snapshot, drop the index, restore it: the restored segments come back from the repository.
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        final var snapshot = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .setIndices(INDEX)
            .get();
        assertEquals(snapshot.getSnapshotInfo().totalShards(), snapshot.getSnapshotInfo().successfulShards());
        assertAcked(indicesAdmin().prepareDelete(INDEX));
        final var restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .setIndices(INDEX)
            .get();
        assertEquals(0, restore.getRestoreInfo().failedShards());
        ensureGreen(INDEX);
        indexBatch();
        assertSortedAfterMerge(INDEX, null);

        // Peer recovery: a replica on a new node is built from the primary's files, plus any operations replayed into segments of its
        // own, and then merges them itself.
        final String replicaNode = internalCluster().startDataOnlyNode();
        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 1), INDEX);
        ensureGreen(INDEX);
        assertSortedAfterMerge(INDEX, replicaNode);

        // Resize: the target's shards are built from the source's segments, which must carry an equal sort. A shrink needs every
        // primary on one node, so they are gathered on the node that took the replicas.
        updateIndexSettings(
            Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", replicaNode)
                .put("index.blocks.write", true),
            INDEX
        );
        ensureGreen(INDEX);
        final String shrunk = INDEX + "-shrunk";
        final String split = INDEX + "-split";
        final String cloned = INDEX + "-cloned";
        assertAcked(executeResize(ResizeType.SHRINK, INDEX, shrunk, indexSettings(1, 0)));
        assertAcked(executeResize(ResizeType.SPLIT, INDEX, split, indexSettings(4, 0)));
        assertAcked(executeResize(ResizeType.CLONE, INDEX, cloned, indexSettings(2, 0)));
        ensureGreen(shrunk, split, cloned);
        for (String target : List.of(shrunk, split, cloned)) {
            assertSortedAfterMerge(target, null);
        }
    }

    /** Indexes one segment's worth of documents of every shape, each document's {@code kw} recorded under its id. */
    private void indexBatch() {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = between(20, 60); i > 0; i--) {
            final String id = Integer.toString(nextId++);
            final List<String> values = switch (between(0, 6)) {
                case 0 -> null;
                case 1 -> List.of();
                case 2 -> Arrays.asList((String) null);
                case 3 -> List.of("term-" + between(0, 4));
                case 4 -> Arrays.asList("term-" + between(0, 4), null, "value-" + randomAlphaOfLength(between(1, 6)));
                case 5 -> List.of("", "term-" + between(0, 2));
                default -> {
                    final List<String> many = new ArrayList<>();
                    for (int v = between(2, 5); v > 0; v--) {
                        many.add(randomBoolean() ? "term-" + between(0, 4) : "value-" + randomAlphaOfLength(between(1, 8)));
                    }
                    yield many;
                }
            };
            final Map<String, Object> source = new LinkedHashMap<>();
            source.put("@timestamp", "2024-01-01T00:00:00Z");
            if (values != null) {
                source.put("kw", values);
            }
            bulk.add(prepareIndex(INDEX).setId(id).setSource(source));
            live.put(id, values);
        }
        assertNoFailures(bulk.get());
    }

    private void deleteSome() {
        for (String id : List.copyOf(live.keySet())) {
            if (randomInt(9) == 0) {
                client().prepareDelete(INDEX, id).get();
                live.remove(id);
            }
        }
        indicesAdmin().prepareRefresh(INDEX).get();
    }

    private void assertSortedAfterMerge(String index, String node) {
        assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
        indicesAdmin().prepareRefresh(index).get();
        assertSorted(index, node);
    }

    /**
     * Each shard of {@code index}, read in index order, holds its documents ordered by their sort key, and together the shards hold
     * exactly the live documents. With {@code node} set, the copies on that node are the ones read.
     */
    private void assertSorted(String index, String node) {
        final String description = "[" + index + "] mode=" + mode + " max=" + max + " desc=" + descending + " missingFirst=" + missingFirst;
        final Comparator<String> byKey = descending ? Comparator.<String>reverseOrder() : Comparator.<String>naturalOrder();
        final Comparator<String> order = missingFirst ? Comparator.nullsFirst(byKey) : Comparator.nullsLast(byKey);
        final Set<String> seen = new HashSet<>();
        for (int shard = 0; shard < getNumShards(index).numPrimaries; shard++) {
            final String preference = "_shards:" + shard + (node == null ? "" : "|_only_nodes:" + node);
            final var response = prepareSearch(index).setPreference(preference)
                .addSort("_doc", SortOrder.ASC)
                .setSize(live.size() + 1)
                .setFetchSource(false)
                .get();
            try {
                String previous = null;
                boolean first = true;
                for (var hit : response.getHits().getHits()) {
                    assertTrue(description + " holds deleted or unknown [" + hit.getId() + "]", live.containsKey(hit.getId()));
                    assertTrue(description + " holds [" + hit.getId() + "] twice", seen.add(hit.getId()));
                    final String key = sortKey(live.get(hit.getId()));
                    if (first == false) {
                        assertTrue(
                            description + " shard " + shard + " has [" + previous + "] before [" + key + "]",
                            order.compare(previous, key) <= 0
                        );
                    }
                    previous = key;
                    first = false;
                }
            } finally {
                response.decRef();
            }
        }
        assertEquals(description, live.keySet(), seen);
    }

    /** The one value of {@code values} that the sort looks at, or {@code null} where the document has none. */
    private String sortKey(List<String> values) {
        if (values == null) {
            return null;
        }
        return values.stream()
            .filter(v -> v != null)
            .reduce((a, b) -> (max ? a.compareTo(b) >= 0 : a.compareTo(b) <= 0) ? a : b)
            .orElse(null);
    }
}
