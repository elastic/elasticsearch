/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.Doc;
import org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.Expectation;
import org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.Kind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.assumeSupported;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.indexDocs;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.loadAllExpectations;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.loadExpectations;
import static org.hamcrest.Matchers.equalTo;

/**
 * Puts indices that map {@code nested_punk.subfield} differently on the same or different nodes, then runs every query under several
 * batch sizes, reductions and node and shard evaluation orders: LOAD and LOAD_ALL must return the same rows under all of them.
 */
public class UnmappedFieldsNestedPlacementIT extends AbstractEsqlIntegTestCase {

    /** More shards than any node holds here, so each node evaluates all its shards in a single batch. */
    private static final int ONE_BATCH = 100;

    /** More nodes than the cluster has, see {@link #pragmas}. */
    private static final int ALL_NODES = 100;

    /** Rotations in which each kind of index leads once. */
    private static final List<List<Kind>> KIND_ORDERS = List.of(
        List.of(Kind.NESTED, Kind.MISSING, Kind.OBJECT),
        List.of(Kind.MISSING, Kind.OBJECT, Kind.NESTED),
        List.of(Kind.OBJECT, Kind.NESTED, Kind.MISSING)
    );

    /**
     * @param nodeOrder the nodes holding shards, in the order they evaluate, see {@link UnmappedFieldsNestedFixture#intercept}
     * @param kindOrder the order each node evaluates its shards in, by the kind of their index
     */
    private record Schedule(int maxConcurrentShardsPerNode, boolean nodeLevelReduction, List<String> nodeOrder, List<Kind> kindOrder) {}

    /** Where each index lives and the documents it holds. */
    private static final class Placement {
        private final Map<String, Kind> kinds = new HashMap<>();
        private final Map<String, String> nodes = new HashMap<>();
        private final List<Doc> docs = new ArrayList<>();

        String from(boolean withObject) {
            return "FROM " + String.join(", ", indices(withObject));
        }

        Set<String> indices(boolean withObject) {
            return kinds.keySet().stream().filter(index -> withObject || kinds.get(index) != Kind.OBJECT).collect(Collectors.toSet());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    public void testLoadSameNode() {
        assertSameNode(false);
    }

    public void testLoadAllSameNode() {
        assertSameNode(true);
    }

    public void testLoadSeparateNodes() {
        assertSeparateNodes(false);
    }

    public void testLoadAllSeparateNodes() {
        assertSeparateNodes(true);
    }

    public void testLoadManyIndices() {
        assertManyIndices(false);
    }

    public void testLoadAllManyIndices() {
        assertManyIndices(true);
    }

    private void assertSameNode(boolean loadAll) {
        assumeSupported(loadAll);
        internalCluster().ensureAtLeastNumDataNodes(2);
        String node = randomDataNode();
        Placement placement = new Placement();
        for (Kind kind : Kind.values()) {
            createAndIndex(placement, kind, kind.name().toLowerCase(Locale.ROOT) + "_" + randomIdentifier(), node);
        }
        assertWithAndWithoutObject(loadAll, placement, node, new int[] { 1, 2, ONE_BATCH }, placement::from);
    }

    private void assertSeparateNodes(boolean loadAll) {
        assumeSupported(loadAll);
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nestedNode = randomDataNode();
        String missingNode = randomValueOtherThan(nestedNode, this::randomDataNode);
        Placement placement = new Placement();
        createAndIndex(placement, Kind.NESTED, "nested_" + randomIdentifier(), nestedNode);
        createAndIndex(placement, Kind.MISSING, "missing_" + randomIdentifier(), missingNode);
        createAndIndex(placement, Kind.OBJECT, "object_" + randomIdentifier(), randomFrom(nestedNode, missingNode));
        assertWithAndWithoutObject(loadAll, placement, nestedNode, new int[] { 1, ONE_BATCH }, placement::from);
    }

    /**
     * Many indices behind the pattern lack {@code nested_punk}, so many batches hold no shard of an index mapping it.
     */
    private void assertManyIndices(boolean loadAll) {
        assumeSupported(loadAll);
        internalCluster().ensureAtLeastNumDataNodes(2);
        String prefix = randomIdentifier() + "_";
        String nestedNode = randomDataNode();
        Placement placement = new Placement();
        createAndIndex(placement, Kind.NESTED, prefix + "nested0", nestedNode);
        createAndIndex(placement, Kind.MISSING, prefix + "missing0", randomValueOtherThan(nestedNode, this::randomDataNode));
        List<Kind> others = new ArrayList<>();
        others.addAll(Collections.nCopies(between(0, 1), Kind.NESTED));
        others.addAll(Collections.nCopies(between(1, 2), Kind.OBJECT));
        others.addAll(Collections.nCopies(between(2, 5), Kind.MISSING));
        for (int i = 0; i < others.size(); i++) {
            Kind kind = others.get(i);
            createAndIndex(placement, kind, prefix + kind.name().toLowerCase(Locale.ROOT) + (i + 1), randomDataNode());
        }
        Function<Boolean, String> from = withObject -> withObject
            ? "FROM " + prefix + "*"
            : "FROM " + prefix + "nested*, " + prefix + "missing*";
        assertWithAndWithoutObject(loadAll, placement, nestedNode, new int[] { 1, 2, ONE_BATCH }, from);
    }

    private void createAndIndex(Placement placement, Kind kind, String index, String node) {
        UnmappedFieldsNestedFixture.createIndex(client(), kind, index, node);
        placement.kinds.put(index, kind);
        placement.nodes.put(index, node);
        placement.docs.addAll(indexDocs(client(), kind, index, between(2, 5)));
    }

    /**
     * Without an object-mapped index the path is unmapped everywhere the coordinator looks; with one it becomes a partially mapped
     * keyword, which LOAD_ALL plans as a column of its own rather than expanding from {@code _source}.
     */
    private void assertWithAndWithoutObject(
        boolean loadAll,
        Placement placement,
        String nestedNode,
        int[] batchSizes,
        Function<Boolean, String> from
    ) {
        for (boolean withObject : new boolean[] { false, true }) {
            Set<String> nodes = placement.indices(withObject).stream().map(placement.nodes::get).collect(Collectors.toSet());
            List<Doc> docs = placement.docs.stream().filter(d -> withObject || d.kind() != Kind.OBJECT).toList();
            String query = from.apply(withObject);
            List<Expectation> expectations = loadAll ? loadAllExpectations(query, docs) : loadExpectations(query, docs);
            for (Schedule schedule : schedules(batchSizes, nodeOrders(nodes, nestedNode))) {
                for (Expectation expectation : expectations) {
                    assertScheduled(schedule, placement.kinds, expectation);
                }
            }
        }
    }

    /**
     * The nested index's node first, then the others, and the reverse.
     */
    private static List<List<String>> nodeOrders(Set<String> nodes, String nestedNode) {
        List<String> nestedNodeFirst = new ArrayList<>(nodes);
        nestedNodeFirst.remove(nestedNode);
        Collections.shuffle(nestedNodeFirst, random());
        nestedNodeFirst.addFirst(nestedNode);
        return nestedNodeFirst.size() == 1 ? List.of(nestedNodeFirst) : List.of(nestedNodeFirst, nestedNodeFirst.reversed());
    }

    private static List<Schedule> schedules(int[] batchSizes, List<List<String>> nodeOrders) {
        List<Schedule> schedules = new ArrayList<>();
        for (int maxConcurrentShards : batchSizes) {
            for (List<String> nodeOrder : nodeOrders) {
                for (List<Kind> kindOrder : KIND_ORDERS) {
                    // Alternating sets both reduction pragmas at every batch size and node order, for half the queries of crossing them.
                    // A data node that also coordinates skips the node-level reduction either way.
                    schedules.add(new Schedule(maxConcurrentShards, schedules.size() % 2 == 0, nodeOrder, kindOrder));
                }
            }
        }
        return schedules;
    }

    private void assertScheduled(Schedule schedule, Map<String, Kind> kinds, Expectation expectation) {
        Map<String, Integer> handled = new ConcurrentHashMap<>();
        try {
            apply(schedule, kinds, handled);
            var request = syncEsqlQueryRequest(expectation.query()).pragmas(pragmas(schedule)).acceptedPragmaRisks(true);
            try (EsqlQueryResponse response = run(request)) {
                expectation.assertMatches(schedule.toString(), response);
            }
            Map<String, Integer> once = schedule.nodeOrder().stream().collect(Collectors.toMap(node -> node, node -> 1));
            assertThat(schedule + " not applied to " + expectation.query(), handled, equalTo(once));
        } finally {
            for (String node : schedule.nodeOrder()) {
                MockTransportService.getInstance(node).clearInboundRules();
            }
        }
    }

    private static void apply(Schedule schedule, Map<String, Kind> kinds, Map<String, Integer> handled) {
        Comparator<DataNodeRequest.Shard> shardOrder = Comparator.<DataNodeRequest.Shard>comparingInt(
            shard -> schedule.kindOrder().indexOf(kinds.get(shard.shardId().getIndexName()))
        ).thenComparing(DataNodeRequest.Shard::shardId);
        SubscribableListener<Void> previous = null;
        for (String node : schedule.nodeOrder()) {
            SubscribableListener<Void> done = new SubscribableListener<>();
            UnmappedFieldsNestedFixture.<DataNodeRequest>intercept(
                MockTransportService.getInstance(node),
                ComputeService.DATA_ACTION_NAME,
                previous,
                done,
                request -> {
                    handled.merge(node, 1, Integer::sum);
                    return withShards(request, shardOrder);
                }
            );
            previous = done;
        }
    }

    /**
     * A node evaluates its shards in request order, {@code max_concurrent_shards_per_node} at a time, so reordering them picks which
     * shards share a batch and which batch runs first.
     */
    private static DataNodeRequest withShards(DataNodeRequest request, Comparator<DataNodeRequest.Shard> order) {
        List<DataNodeRequest.Shard> shards = new ArrayList<>(request.shards());
        shards.sort(order);
        DataNodeRequest reordered = new DataNodeRequest(
            request.sessionId(),
            request.configuration(),
            request.clusterAlias(),
            shards,
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            request.retainSearchContexts(),
            request.singleNodeOptimizations(),
            request.externalSplits()
        );
        reordered.setParentTask(request.getParentTask());
        return reordered;
    }

    private static QueryPragmas pragmas(Schedule schedule) {
        return new QueryPragmas(
            Settings.builder()
                .put(randomPragmas().getSettings())
                .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), schedule.maxConcurrentShardsPerNode())
                .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), schedule.nodeLevelReduction())
                // A node held back by the schedule keeps its request slot, so all nodes must be asked at once.
                .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), ALL_NODES)
                .build()
        );
    }

    private String randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values()).getName();
    }
}
