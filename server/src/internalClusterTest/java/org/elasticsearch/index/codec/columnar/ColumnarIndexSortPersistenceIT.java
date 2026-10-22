/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * A columnar index sorted by a field keeps its sort through every path that rebuilds or reopens its segments from disk: close and
 * reopen, snapshot and restore, peer recovery to a replica, and shrink, split and clone. Each of these reads the index sort back out of
 * the segment metadata, where some sort fields, such as the binary one a keyword sorts on, are serialized by their own provider, and
 * resize additionally requires the sort of the copied segments to equal the target's. A sort that failed to round-trip would either fail
 * the operation or leave segments, and the merges built on them, out of order without any error.
 *
 * <p>The scenario is the same for every field type that can sort an index, so each type is one test method handing it the field's
 * mapping type, a source of values and the order the sort should put them in. None of it depends on which doc-values format the
 * index's codec picks for the field.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ColumnarIndexSortPersistenceIT extends ESIntegTestCase {

    private static final String INDEX = "sorted";
    private static final String FIELD = "f";

    /** Every test runs in both strictly columnar modes rather than one picked at random. */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { IndexMode.COLUMNAR }, new Object[] { IndexMode.LOGSDB_COLUMNAR });
    }

    private final IndexMode mode;

    public ColumnarIndexSortPersistenceIT(@Name("mode") IndexMode mode) {
        this.mode = mode;
    }

    /** A field type that can sort an index: its mapping type, a random value for it, and the order the sort puts its values in. */
    private record FieldCase<T>(String type, Supplier<T> randomValue, Comparator<T> order) {}

    private static final FieldCase<String> KEYWORD = new FieldCase<>(
        "keyword",
        () -> randomBoolean() ? "term-" + between(0, 4) : "value-" + randomAlphaOfLength(between(0, 8)),
        Comparator.naturalOrder()
    );

    /** Addresses sort by their 16-byte form, so every IPv4 address, held as IPv4-mapped IPv6, sorts before any other IPv6 one. */
    private static final FieldCase<String> IP = new FieldCase<>(
        "ip",
        () -> NetworkAddress.format(randomIp(randomBoolean())),
        (a, b) -> Arrays.compareUnsigned(
            InetAddressPoint.encode(InetAddresses.forString(a)),
            InetAddressPoint.encode(InetAddresses.forString(b))
        )
    );

    public void testKeyword() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(KEYWORD, false);
    }

    public void testKeywordWithIndexLevelSingleValue() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(KEYWORD, true);
    }

    public void testIp() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(IP, false);
    }

    public void testIpWithIndexLevelSingleValue() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(IP, true);
    }

    public void testLong() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("long", () -> randomBoolean() ? randomLongBetween(-5, 5) : randomLong(), Comparator.<Long>naturalOrder()),
            false
        );
    }

    public void testInteger() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("integer", () -> randomBoolean() ? between(-5, 5) : randomInt(), Comparator.<Integer>naturalOrder()),
            false
        );
    }

    public void testShort() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("short", () -> randomShort(), Comparator.<Short>naturalOrder()),
            false
        );
    }

    public void testByte() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("byte", () -> randomByte(), Comparator.<Byte>naturalOrder()),
            false
        );
    }

    public void testDouble() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("double", () -> randomDoubleBetween(-1000, 1000, true), Comparator.<Double>naturalOrder()),
            false
        );
    }

    /** Eighths of an integer, which a float holds exactly, so the value the sort reads is the value indexed. */
    public void testFloat() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("float", () -> between(-100_000, 100_000) / 8f, Comparator.<Float>naturalOrder()),
            false
        );
    }

    public void testDate() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("date", () -> randomLongBetween(0, 4_102_444_800_000L), Comparator.<Long>naturalOrder()),
            false
        );
    }

    public void testDateNanos() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("date_nanos", () -> randomLongBetween(0, 4_102_444_800_000L), Comparator.<Long>naturalOrder()),
            false
        );
    }

    public void testBoolean() throws Exception {
        assertSortSurvivesReopenRestoreRecoveryAndResize(
            new FieldCase<>("boolean", () -> randomBoolean(), Comparator.<Boolean>naturalOrder()),
            false
        );
    }

    /** The documents of one run, each recorded under its id with its field values, {@code null} where it has no field at all. */
    private static final class Run<T> {
        final FieldCase<T> field;
        final boolean singleValued;
        final boolean max = randomBoolean();
        final boolean descending = randomBoolean();
        final boolean missingFirst = randomBoolean();
        final Map<String, List<T>> live = new HashMap<>();
        int nextId = 0;

        Run(FieldCase<T> field, boolean singleValued) {
            this.field = field;
            this.singleValued = singleValued;
        }

        /** The one value of {@code values} that the sort looks at, or {@code null} where the document has none. */
        T sortKey(List<T> values) {
            if (values == null) {
                return null;
            }
            return values.stream()
                .filter(v -> v != null)
                .reduce((a, b) -> (max ? field.order.compare(a, b) >= 0 : field.order.compare(a, b) <= 0) ? a : b)
                .orElse(null);
        }

        String description(String index, IndexMode mode) {
            return "["
                + index
                + "] mode="
                + mode
                + " type="
                + field.type
                + " singleValued="
                + singleValued
                + " max="
                + max
                + " desc="
                + descending
                + " missingFirst="
                + missingFirst;
        }
    }

    /**
     * With {@code singleValued} the index holds every field to one value per document through {@code index.mapping.doc_values.multi_value},
     * rather than the field doing so itself.
     */
    private <T> void assertSortSurvivesReopenRestoreRecoveryAndResize(FieldCase<T> field, boolean singleValued) throws Exception {
        final Run<T> run = new Run<>(field, singleValued);
        final Settings.Builder settings = indexSettings(2, 0).put(IndexSettings.MODE.getKey(), mode)
            .put("index.number_of_routing_shards", 4)
            .put("index.sort.field", FIELD)
            .put("index.sort.mode", run.max ? "max" : "min")
            .put("index.sort.order", run.descending ? "desc" : "asc")
            .put("index.sort.missing", run.missingFirst ? "_first" : "_last");
        if (singleValued) {
            settings.put("index.mapping.doc_values.multi_value", false);
        }
        assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(settings).setMapping(Strings.format("""
            {"properties":{"@timestamp":{"type":"date"},"%s":{"type":"%s"}}}""", FIELD, field.type)));
        ensureGreen(INDEX);
        for (int batch = between(2, 4); batch > 0; batch--) {
            indexBatch(run);
        }
        deleteSome(run);
        assertSortedAfterMerge(run, INDEX, null);

        // Close and reopen: the shards open their segments from disk again.
        assertAcked(indicesAdmin().prepareClose(INDEX));
        assertAcked(indicesAdmin().prepareOpen(INDEX));
        ensureGreen(INDEX);
        indexBatch(run);
        assertSortedAfterMerge(run, INDEX, null);

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
        indexBatch(run);
        assertSortedAfterMerge(run, INDEX, null);

        // Peer recovery: a replica on a new node is built from the primary's files, which the primary has just merged to one segment.
        // A batch indexed once it is green gives it a segment of its own, so its merge has to put the copied segment and that one
        // together by the sort it read back from the copy.
        final String replicaNode = internalCluster().startDataOnlyNode();
        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 1), INDEX);
        ensureGreen(INDEX);
        indexBatch(run);
        assertSortedAfterMerge(run, INDEX, replicaNode);

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
            assertSortedAfterMerge(run, target, null);
        }
    }

    /**
     * Indexes one segment's worth of documents of every shape the index accepts, each document's values recorded under its id: no field,
     * one value, and unless the index is single-valued, an empty array, only a null, and several values with a null among them.
     */
    private <T> void indexBatch(Run<T> run) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = between(20, 60); i > 0; i--) {
            final String id = Integer.toString(run.nextId++);
            final List<T> values = switch (between(0, run.singleValued ? 1 : 5)) {
                case 0 -> null;
                case 1 -> List.of(run.field.randomValue.get());
                case 2 -> List.of();
                case 3 -> Collections.<T>singletonList(null);
                case 4 -> Arrays.asList(run.field.randomValue.get(), null, run.field.randomValue.get());
                default -> {
                    final List<T> many = new ArrayList<>();
                    for (int v = between(2, 5); v > 0; v--) {
                        many.add(run.field.randomValue.get());
                    }
                    yield many;
                }
            };
            final Map<String, Object> source = new LinkedHashMap<>();
            source.put("@timestamp", "2024-01-01T00:00:00Z");
            if (values != null) {
                source.put(FIELD, run.singleValued ? values.getFirst() : values);
            }
            bulk.add(prepareIndex(INDEX).setId(id).setSource(source));
            run.live.put(id, values);
        }
        assertNoFailures(bulk.get());
    }

    private <T> void deleteSome(Run<T> run) {
        for (String id : List.copyOf(run.live.keySet())) {
            if (randomInt(9) == 0) {
                client().prepareDelete(INDEX, id).get();
                run.live.remove(id);
            }
        }
        indicesAdmin().prepareRefresh(INDEX).get();
    }

    private <T> void assertSortedAfterMerge(Run<T> run, String index, String node) {
        assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
        indicesAdmin().prepareRefresh(index).get();
        assertSorted(run, index, node);
    }

    /**
     * Each shard of {@code index}, read in index order, holds its documents ordered by their sort key, and together the shards hold
     * exactly the live documents. With {@code node} set, the copies on that node are the ones read.
     */
    private <T> void assertSorted(Run<T> run, String index, String node) {
        final String description = run.description(index, mode);
        final Comparator<T> byKey = run.descending ? run.field.order.reversed() : run.field.order;
        final Comparator<T> order = run.missingFirst ? Comparator.nullsFirst(byKey) : Comparator.nullsLast(byKey);
        final Set<String> seen = new HashSet<>();
        for (int shard = 0; shard < getNumShards(index).numPrimaries; shard++) {
            final String preference = "_shards:" + shard + (node == null ? "" : "|_only_nodes:" + node);
            final var response = prepareSearch(index).setPreference(preference)
                .addSort("_doc", SortOrder.ASC)
                .setSize(run.live.size() + 1)
                .setFetchSource(false)
                .get();
            try {
                T previous = null;
                boolean first = true;
                for (var hit : response.getHits().getHits()) {
                    assertTrue(description + " holds deleted or unknown [" + hit.getId() + "]", run.live.containsKey(hit.getId()));
                    assertTrue(description + " holds [" + hit.getId() + "] twice", seen.add(hit.getId()));
                    final T key = run.sortKey(run.live.get(hit.getId()));
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
        assertEquals(description, run.live.keySet(), seen);
    }
}
