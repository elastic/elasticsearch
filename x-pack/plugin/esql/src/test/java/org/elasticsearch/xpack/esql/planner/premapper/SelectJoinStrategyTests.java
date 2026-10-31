/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.ColocatedJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Choosing the execution strategy of the {@code JOIN} command against a hand-built cluster state: three nodes
 * ({@code node_0} to {@code node_2}) and two three-shard indices, {@code events} and {@code sessions}, by default with
 * shard {@code i} of both on {@code node_i}, {@code _routing} required and {@code index.routing_field: session_id}. Only
 * the right index ({@code sessions}) has to be routed by the join key.
 */
public class SelectJoinStrategyTests extends ESTestCase {
    private static final String ROUTED = """
        { "_doc": { "_routing": { "required": true } } }""";

    private final FieldAttribute key = getFieldAttribute("session_id");
    private final FieldAttribute rightKey = getFieldAttribute("session_id");
    private final FieldAttribute seq = getFieldAttribute("seq");
    private final FieldAttribute country = getFieldAttribute("country");

    public void testAlignedIndicesAreColocated() {
        LogicalPlan plan = SelectJoinStrategy.apply(join(relation("events", key, seq), relation("sessions", rightKey, country)), state());
        ColocatedJoin join = as(plan, ColocatedJoin.class);
        assertEquals(ExecuteLocation.REMOTE, join.executesOn());
    }

    /**
     * Streaming commands below the join stay inside the fragment and run per shard, which is fine.
     */
    public void testStreamingCommandsBelowJoin() {
        EsRelation events = relation("events", key, seq);
        LogicalPlan left = new Project(EMPTY, new Filter(EMPTY, events, greaterThanOf(seq, ONE)), List.of(key, seq));
        LogicalPlan right = new Filter(EMPTY, relation("sessions", rightKey, country), greaterThanOf(country, of("DE")));
        as(SelectJoinStrategy.apply(join(left, right), state()), ColocatedJoin.class);
    }

    public void testRejectsLimitBelowJoin() {
        LogicalPlan left = new Limit(EMPTY, of(5), relation("events", key, seq));
        expectFailure(join(left, relation("sessions", rightKey, country)), state(), "JOIN can't be executed after [] on the left side");
    }

    /**
     * Shard {@code i} of both indices on different nodes: the lookups of those shards go to another node, which is slower but
     * correct.
     */
    public void testMisplacedShardsAreAccepted() {
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 3, Settings.EMPTY, ROUTED),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0", "node_2", "node_2" }
        );
        as(
            SelectJoinStrategy.apply(join(relation("events", key, seq), relation("sessions", rightKey, country)), state),
            ColocatedJoin.class
        );
    }

    public void testDifferentShardCountsAreAccepted() {
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 1, Settings.EMPTY, ROUTED),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0" }
        );
        as(
            SelectJoinStrategy.apply(join(relation("events", key, seq), relation("sessions", rightKey, country)), state),
            ColocatedJoin.class
        );
    }

    /**
     * Only the right index has to be routed by the join key: the left one can route documents by {@code _id}, with no
     * routing field, required routing or any particular shard count.
     */
    public void testUnroutedLeftIndexIsAccepted() {
        ProjectState state = state(
            index("events", 2, Settings.builder().put(IndexSettings.ROUTING_FIELD.getKey(), "").build(), null),
            index("sessions", 3, Settings.EMPTY, ROUTED),
            new String[] { "node_0", "node_1" },
            new String[] { "node_0", "node_1", "node_2" }
        );
        as(
            SelectJoinStrategy.apply(join(relation("events", key, seq), relation("sessions", rightKey, country)), state),
            ColocatedJoin.class
        );
    }

    /**
     * The left index is routed, but by another field than the join key: fine, its rows are routed to the right shards by key.
     */
    public void testLeftRoutedByOtherFieldIsAccepted() {
        ProjectState state = state(
            index("events", 3, Settings.builder().put(IndexSettings.ROUTING_FIELD.getKey(), "user_id").build(), ROUTED),
            index("sessions", 3, Settings.EMPTY, ROUTED),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0", "node_1", "node_2" }
        );
        as(
            SelectJoinStrategy.apply(join(relation("events", key, seq), relation("sessions", rightKey, country)), state),
            ColocatedJoin.class
        );
    }

    /**
     * The left side can read several indices.
     */
    public void testSeveralLeftIndicesAreAccepted() {
        EsRelation events = new EsRelation(
            EMPTY,
            "events*",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of("events", "events_unrouted")),
            Map.of(),
            List.of(key, seq)
        );
        as(SelectJoinStrategy.apply(join(events, relation("sessions", rightKey, country)), state()), ColocatedJoin.class);
    }

    public void testRejectsRemoteLeftIndices() {
        EsRelation events = new EsRelation(
            EMPTY,
            "events,remote:events",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of("events"), "remote", List.of("events")),
            Map.of(),
            List.of(key, seq)
        );
        expectFailure(
            join(events, relation("sessions", rightKey, country)),
            state(),
            "JOIN requires [events,remote:events] to resolve to local indices only"
        );
    }

    public void testRejectsRoutingNotRequired() {
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 3, Settings.EMPTY, null),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0", "node_1", "node_2" }
        );
        expectFailure(
            join(relation("events", key, seq), relation("sessions", rightKey, country)),
            state,
            "JOIN requires [sessions] to have [_routing] required in its mapping"
        );
    }

    /**
     * A partitioned index cannot declare a routing field, so it fails both checks.
     */
    public void testRejectsRoutingPartitionsOnTheRight() {
        Settings partitioned = Settings.builder()
            .put(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey(), 2)
            .put(IndexSettings.ROUTING_FIELD.getKey(), "")
            .build();
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 3, partitioned, ROUTED),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0", "node_1", "node_2" }
        );
        expectFailure(
            join(relation("events", key, seq), relation("sessions", rightKey, country)),
            state,
            "JOIN does not support [sessions]: it uses [index.routing_partition_size]"
        );
    }

    public void testRejectsIndexWithoutRoutingField() {
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 3, Settings.builder().put(IndexSettings.ROUTING_FIELD.getKey(), "").build(), ROUTED),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0", "node_1", "node_2" }
        );
        expectFailure(
            join(relation("events", key, seq), relation("sessions", rightKey, country)),
            state,
            "JOIN requires [sessions] to set [index.routing_field] to the join key [session_id]"
        );
    }

    /**
     * The right index routes by another field than the join key: a key's matches can be on any shard.
     */
    public void testRejectsJoinOnOtherField() {
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 3, Settings.builder().put(IndexSettings.ROUTING_FIELD.getKey(), "user_id").build(), ROUTED),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0", "node_1", "node_2" }
        );
        expectFailure(
            join(relation("events", key, seq), relation("sessions", rightKey, country)),
            state,
            "JOIN requires the join key [session_id] to be the routing field of [sessions], found [index.routing_field: user_id]"
        );
    }

    /**
     * Every failed condition of a join is reported, not just the first.
     */
    public void testReportsEveryReason() {
        ProjectState state = state(
            index("events", 3, Settings.EMPTY, ROUTED),
            index("sessions", 1, Settings.builder().put(IndexSettings.ROUTING_FIELD.getKey(), "").build(), null),
            new String[] { "node_0", "node_1", "node_2" },
            new String[] { "node_0" }
        );
        LogicalPlan plan = join(new Limit(EMPTY, of(5), relation("events", key, seq)), relation("sessions", rightKey, country));
        VerificationException e = expectThrows(VerificationException.class, () -> SelectJoinStrategy.apply(plan, state));
        assertThat(e.getMessage(), containsString("JOIN requires [sessions] to have [_routing] required in its mapping"));
        assertThat(e.getMessage(), containsString("JOIN requires [sessions] to set [index.routing_field] to the join key [session_id]"));
        assertThat(e.getMessage(), containsString("JOIN can't be executed after [] on the left side"));
    }

    /**
     * Joins that are not the {@code JOIN} command are left alone.
     */
    public void testIgnoresOtherJoins() {
        Join lookupStyle = new Join(
            EMPTY,
            relation("events", key, seq),
            relation("sessions", rightKey, country),
            new JoinConfig(JoinTypes.LEFT, List.of(key), List.of(rightKey), null),
            ExecuteLocation.ANY
        );
        LogicalPlan plan = SelectJoinStrategy.apply(lookupStyle, state());
        assertThat(plan, not(instanceOf(ColocatedJoin.class)));
        assertEquals(lookupStyle, plan);
    }

    private void expectFailure(LogicalPlan plan, ProjectState state, String message) {
        VerificationException e = expectThrows(VerificationException.class, () -> SelectJoinStrategy.apply(plan, state));
        assertThat(e.getMessage(), containsString(message));
    }

    private Join join(LogicalPlan left, LogicalPlan right) {
        Join join = new Join(
            EMPTY,
            left,
            right,
            new JoinConfig(JoinTypes.INNER, List.of(key), List.of(rightKey), null),
            ExecuteLocation.ANY
        );
        assertTrue(join.isCandicateColocatedJoin());
        return join;
    }

    private static EsRelation relation(String index, FieldAttribute... fields) {
        return new EsRelation(
            EMPTY,
            index,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of(index)),
            Map.of(),
            List.of(fields)
        );
    }

    private static ProjectState state() {
        String[] aligned = new String[] { "node_0", "node_1", "node_2" };
        return state(index("events", 3, Settings.EMPTY, ROUTED), index("sessions", 3, Settings.EMPTY, ROUTED), aligned, aligned);
    }

    private static IndexMetadata index(String name, int shards, Settings extra, String mapping) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(indexSettings(IndexVersion.current(), shards, 0).put(IndexSettings.ROUTING_FIELD.getKey(), "session_id").put(extra));
        if (mapping != null) {
            builder.putMapping(mapping);
        }
        return builder.build();
    }

    /**
     * Started primaries of both indices, shard {@code i} on the node at position {@code i} of the given arrays.
     */
    private static ProjectState state(IndexMetadata left, IndexMetadata right, String[] leftNodes, String[] rightNodes) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            nodes.add(DiscoveryNodeUtils.builder("node_" + i).name("node_" + i).build());
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(Metadata.builder().put(left, false).put(right, false))
            .routingTable(RoutingTable.builder().add(routing(left, leftNodes)).add(routing(right, rightNodes)))
            .build()
            .projectState();
    }

    private static IndexRoutingTable routing(IndexMetadata index, String[] nodes) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index.getIndex());
        for (int shard = 0; shard < index.getNumberOfShards(); shard++) {
            builder.addShard(
                TestShardRouting.newShardRouting(new ShardId(index.getIndex(), shard), nodes[shard], true, ShardRoutingState.STARTED)
            );
        }
        return builder.build();
    }
}
