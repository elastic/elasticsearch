/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.DiffableTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.common.util.Maps.transformValues;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GlobalRoutingTableTests extends AbstractWireSerializingTestCase<GlobalRoutingTable> {

    private static final TransportVersion PRE_MULTI_PROJECT_TRANSPORT_VERSION = TransportVersionUtils.getPreviousVersion(
        TransportVersions.MULTI_PROJECT
    );

    /**
     * We intentionally don't want production code comparing two routing tables for equality.
     * But our unit testing frameworks assume that serialized object can be tested for equality
     */
    public static class GlobalRoutingTableWithEquals extends GlobalRoutingTable {

        public GlobalRoutingTableWithEquals(long version, ImmutableOpenMap<ProjectId, RoutingTable> routingTable) {
            super(routingTable);
        }

        public GlobalRoutingTableWithEquals(GlobalRoutingTable other) {
            super(other.routingTables());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof GlobalRoutingTable that) {
                return GlobalRoutingTableWithEquals.equals(this, that);
            }
            return false;
        }

        static boolean equals(GlobalRoutingTable left, GlobalRoutingTable right) {
            if (left.size() != right.size()) {
                return false;
            }
            if (left.routingTables().keySet().containsAll(right.routingTables().keySet()) == false) {
                return false;
            }
            return left.routingTables()
                .keySet()
                .stream()
                .allMatch(projectId -> equals(left.routingTable(projectId), right.routingTable(projectId)));
        }

        static boolean equals(RoutingTable left, RoutingTable right) {
            return Objects.equals(left.indicesRouting(), right.indicesRouting());
        }

        @Override
        public int hashCode() {
            int result = 0;
            // This is not pretty, but it's necessary because ImmutableOpenMap does not guarantee that the iteration order is identical
            // for separate instances with the same elements
            final var iterator = routingTables().entrySet().stream().sorted(Comparator.comparing(e -> e.getKey().id())).iterator();
            while (iterator.hasNext()) {
                final Map.Entry<ProjectId, RoutingTable> entry = iterator.next();
                result = 31 * result + Objects.hash(entry.getKey(), entry.getValue().indicesRouting());
            }
            return result;
        }

        @Override
        public Diff<GlobalRoutingTable> diff(GlobalRoutingTable previousState) {
            return super.diff(previousState);
        }

        @Override
        public String toString() {
            return "<test:" + super.toString() + ">";
        }
    }

    public void testNonEmptyDiff() throws Exception {
        DiffableTestUtils.testDiffableSerialization(
            this::createTestInstance,
            this::mutateInstance,
            getNamedWriteableRegistry(),
            instanceReader(),
            GlobalRoutingTable::readDiffFrom,
            null,
            GlobalRoutingTableWithEquals::equals
        );
    }

    public void testEmptyDiff() throws Exception {
        DiffableTestUtils.testDiffableSerialization(
            this::createTestInstance,
            Function.identity(),
            getNamedWriteableRegistry(),
            instanceReader(),
            GlobalRoutingTable::readDiffFrom,
            null,
            GlobalRoutingTableWithEquals::equals
        );
    }

    public final void testDiffSerializationPreMultiProject() throws IOException {
        // BWC serialization only works for a routing table with a single project
        final Function<GlobalRoutingTable, GlobalRoutingTable> mutator = instance -> new GlobalRoutingTable(
            ImmutableOpenMap.builder(transformValues(instance.routingTables(), this::mutate)).build()
        );
        DiffableTestUtils.testDiffableSerialization(
            () -> testRoutingTable(1),
            mutator,
            getNamedWriteableRegistry(),
            instanceReader(),
            GlobalRoutingTable::readDiffFrom,
            PRE_MULTI_PROJECT_TRANSPORT_VERSION,
            (original, reconstructed) -> {
                // The round-trip will lose the version of the global table and replace it with the version from the inner routing table
                return GlobalRoutingTableWithEquals.equals(original.getRoutingTable(), reconstructed.getRoutingTable());
            }
        );
    }

    public void testHasSameIndexRouting() {
        final GlobalRoutingTable original = randomValueOtherThanMany(
            grt -> grt.totalIndexCount() == 0, // The mutation below assume that there is at least 1 index
            () -> this.testRoutingTable(randomIntBetween(1, 5))
        );

        // Exactly the same projects => same routing
        GlobalRoutingTable updated = new GlobalRoutingTable(original.routingTables());
        assertTrue(original.hasSameIndexRouting(updated));
        assertTrue(updated.hasSameIndexRouting(original));

        // Updated projects but with same routing => same routing
        updated = new GlobalRoutingTable(
            ImmutableOpenMap.builder(transformValues(original.routingTables(), rt -> RoutingTable.builder(rt).build())).build()
        );
        assertTrue(original.hasSameIndexRouting(updated));
        assertTrue(updated.hasSameIndexRouting(original));

        // Reconstructed index map (with same elements) => different routing
        updated = new GlobalRoutingTable(ImmutableOpenMap.builder(transformValues(original.routingTables(), rt -> {
            final RoutingTable.Builder builder = RoutingTable.builder();
            rt.indicesRouting().values().forEach(builder::add);
            return builder.build();
        })).build());
        assertFalse(original.hasSameIndexRouting(updated));
        assertFalse(updated.hasSameIndexRouting(original));

        // Mutated routing table => different routing
        updated = new GlobalRoutingTable(mutate(original.routingTables()));
        assertFalse(original.hasSameIndexRouting(updated));
        assertFalse(updated.hasSameIndexRouting(original));
    }

    public void testInitializeProjects() {
        final GlobalRoutingTableWithEquals table1 = this.testRoutingTable(randomIntBetween(3, 5));
        assertThat(table1.initializeProjects(Set.of()), sameInstance(table1));
        assertThat(table1.initializeProjects(Set.copyOf(randomSubsetOf(table1.routingTables().keySet()))), sameInstance(table1));

        Set<ProjectId> addition = randomSet(
            1,
            5,
            () -> randomValueOtherThanMany(table1.routingTables()::containsKey, ESTestCase::randomUniqueProjectId)
        );
        var table2 = table1.initializeProjects(addition);
        assertThat(table2, not(sameInstance(table1)));
        assertThat(table2.size(), equalTo(table1.size() + addition.size()));
        for (var p : table1.routingTables().keySet()) {
            assertThat(table2.routingTable(p), sameInstance(table1.routingTable(p)));
        }
        for (var p : addition) {
            assertThat(table2.routingTable(p), sameInstance(RoutingTable.EMPTY_ROUTING_TABLE));
        }
    }

    public void testBuilderFromEmpty() {
        final int numberOfProjects = randomIntBetween(1, 10);
        final ProjectId[] projectIds = randomArray(numberOfProjects, numberOfProjects, ProjectId[]::new, ESTestCase::randomUniqueProjectId);
        final Integer[] projectIndexCount = randomArray(numberOfProjects, numberOfProjects, Integer[]::new, () -> randomIntBetween(0, 12));

        final GlobalRoutingTable.Builder builder = GlobalRoutingTable.builder();
        for (int i = 0; i < numberOfProjects; i++) {
            builder.put(projectIds[i], addIndices(projectIndexCount[i], RoutingTable.builder()));
        }
        final GlobalRoutingTable builtRoutingTable = builder.build();

        assertThat(builtRoutingTable.size(), equalTo(numberOfProjects));
        assertThat(builtRoutingTable.routingTables().keySet(), containsInAnyOrder(projectIds));
        for (int i = 0; i < numberOfProjects; i++) {
            final ProjectId projectId = projectIds[i];
            assertThat(builtRoutingTable.routingTables(), hasKey(projectId));
            final RoutingTable projectRoutingTable = builtRoutingTable.routingTable(projectId);
            assertThat(projectRoutingTable.indicesRouting().size(), equalTo(projectIndexCount[i]));
        }

        final int expectedIndexCount = Arrays.stream(projectIndexCount).mapToInt(Integer::intValue).sum();
        assertThat(builtRoutingTable.totalIndexCount(), equalTo(expectedIndexCount));
    }

    public void testBuilderFromExisting() {
        final GlobalRoutingTable initial = createTestInstance();

        {
            final var instance = GlobalRoutingTable.builder(initial).build();
            assertTrue("Expected " + initial + " to equal " + instance, GlobalRoutingTableWithEquals.equals(initial, instance));
        }

        {
            final var instance = GlobalRoutingTable.builder(initial).clear().build();
            assertThat(instance.routingTables(), anEmptyMap());
        }

        {
            final ProjectId projectId = randomProjectIdOrDefault();
            final RoutingTable projectRouting = randomRoutingTable();
            final var instance = GlobalRoutingTable.builder(initial).put(projectId, projectRouting).build();
            assertThat(instance.routingTables(), aMapWithSize(initial.size() + 1));
            assertThat(instance.routingTables(), hasEntry(projectId, projectRouting));
            initial.routingTables().forEach((id, rt) -> assertThat(instance.routingTables(), hasEntry(id, rt)));
        }

    }

    public void testRoutingNodesRoundtrip() {
        final ClusterState clusterState = buildClusterState(
            Map.ofEntries(
                Map.entry(ProjectId.fromId(randomAlphaOfLength(11) + "1"), Set.of("test-a", "test-b", "test-c")),
                Map.entry(ProjectId.fromId(randomAlphaOfLength(11) + "2"), Set.of("test-a", "test-z"))
            )
        );

        final GlobalRoutingTable originalTable = clusterState.globalRoutingTable();
        final RoutingNodes routingNodes = clusterState.getRoutingNodes();
        final GlobalRoutingTable fromNodes = originalTable.rebuild(routingNodes, clusterState.metadata());
        final Diff<GlobalRoutingTable> routingTableDiff = fromNodes.diff(originalTable);
        assertSame(originalTable, routingTableDiff.apply(originalTable));
    }

    public void testRebuildAfterShardInitialized() {
        final ProjectId project1 = ProjectId.fromId(randomAlphaOfLength(11) + "1");
        final ProjectId project2 = ProjectId.fromId(randomAlphaOfLength(11) + "2");
        final ClusterState clusterState = buildClusterState(
            Map.ofEntries(Map.entry(project1, Set.of("test-a", "test-b", "test-c")), Map.entry(project2, Set.of("test-b", "test-z")))
        );

        final GlobalRoutingTable originalTable = clusterState.globalRoutingTable();
        final RoutingNodes routingNodes = clusterState.getRoutingNodes();

        final RoutingNodes mutate = routingNodes.mutableCopy();
        final DiscoveryNode targetNode = randomFrom(clusterState.nodes().getNodes().values());
        final RoutingChangesObserver emptyObserver = new RoutingChangesObserver() {
        };

        final int unassigned = mutate.unassigned().size();
        var unassignedItr = mutate.unassigned().iterator();
        while (unassignedItr.hasNext()) {
            var shard = unassignedItr.next();
            assertThat(shard, notNullValue());
            if (shard.index().getName().equals("test-a")) {
                // "test-a" only exists in project 1, so we know which project routing table should change
                // (and which one should stay the same)
                unassignedItr.initialize(targetNode.getId(), null, 0L, emptyObserver);
                break;
            }
        }
        assertThat(mutate.unassigned().size(), equalTo(unassigned - 1));
        final GlobalRoutingTable fromNodes = originalTable.rebuild(mutate, clusterState.metadata());
        final Diff<GlobalRoutingTable> routingTableDiff = fromNodes.diff(originalTable);
        final GlobalRoutingTable updatedRouting = routingTableDiff.apply(originalTable);
        assertThat(updatedRouting, not(sameInstance(originalTable)));

        assertThat(updatedRouting.routingTable(project1), not(sameInstance(originalTable.routingTable(project1))));
        assertThat(updatedRouting.routingTable(project2), sameInstance(originalTable.routingTable(project2)));
    }

    private ClusterState buildClusterState(Map<ProjectId, Set<String>> projectIndices) {
        final Metadata.Builder mb = Metadata.builder();

        projectIndices.forEach((projectId, indexNames) -> {
            final ProjectMetadata.Builder project = ProjectMetadata.builder(projectId);
            for (var indexName : indexNames) {
                final IndexMetadata.Builder index = createIndexMetadata(indexName);
                project.put(index);
            }
            mb.put(project);
        });
        final Metadata metadata = mb.build();

        final ImmutableOpenMap.Builder<ProjectId, RoutingTable> routingTables = ImmutableOpenMap.builder(projectIndices.size());
        projectIndices.forEach((projectId, indexNames) -> {
            final RoutingTable.Builder rt = new RoutingTable.Builder();
            for (var indexName : indexNames) {
                final IndexMetadata indexMetadata = metadata.getProject(projectId).index(indexName);
                final IndexRoutingTable indexRouting = new IndexRoutingTable.Builder(
                    TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
                    indexMetadata.getIndex()
                ).initializeAsNew(indexMetadata).build();
                rt.add(indexRouting);
            }
            routingTables.put(projectId, rt.build());
        });
        GlobalRoutingTable globalRoutingTable = new GlobalRoutingTable(routingTables.build());

        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().add(buildRandomDiscoveryNode()).add(buildRandomDiscoveryNode());
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(globalRoutingTable).nodes(nodes).build();
    }

    private DiscoveryNode buildRandomDiscoveryNode() {
        return DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID(random()))
            .name(randomAlphaOfLength(10))
            .ephemeralId(UUIDs.randomBase64UUID(random()))
            .build();
    }

    private IndexMetadata.Builder createIndexMetadata(String indexName) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
            .build();
        return new IndexMetadata.Builder(indexName).settings(indexSettings)
            .numberOfReplicas(randomIntBetween(0, 2))
            .numberOfShards(randomIntBetween(1, 5));
    }

    @Override
    protected Writeable.Reader<GlobalRoutingTable> instanceReader() {
        return in -> {
            var table = GlobalRoutingTable.readFrom(in);
            return new GlobalRoutingTableWithEquals(table);
        };
    }

    @Override
    protected GlobalRoutingTable createTestInstance() {
        return testRoutingTable(randomIntBetween(0, 10));
    }

    private GlobalRoutingTableWithEquals testRoutingTable(int projectCount) {
        Map<ProjectId, RoutingTable> map = randomMap(
            projectCount,
            projectCount,
            () -> new Tuple<>(randomUniqueProjectId(), randomRoutingTable())
        );
        return new GlobalRoutingTableWithEquals(randomLong(), ImmutableOpenMap.builder(map).build());
    }

    @Override
    protected GlobalRoutingTable mutateInstance(GlobalRoutingTable instance) {
        return new GlobalRoutingTable(mutate(instance.routingTables()));
    }

    private ImmutableOpenMap<ProjectId, RoutingTable> mutate(ImmutableOpenMap<ProjectId, RoutingTable> routingTables) {
        if (routingTables.isEmpty()) {
            return ImmutableOpenMap.builder(Map.of(randomProjectIdOrDefault(), randomRoutingTable())).build();
        }
        final Set<ProjectId> existingProjects = routingTables.keySet();
        final ImmutableOpenMap.Builder<ProjectId, RoutingTable> builder = ImmutableOpenMap.builder(routingTables);
        switch (randomIntBetween(1, 3)) {
            case 1 -> {
                final var project = randomFrom(existingProjects);
                final var modified = mutate(routingTables.get(project));
                builder.put(project, modified);
            }
            case 2 -> {
                final var project = randomValueOtherThanMany(existingProjects::contains, GlobalRoutingTableTests::randomProjectIdOrDefault);
                final var routingTable = randomRoutingTable();
                builder.put(project, routingTable);
            }
            case 3 -> {
                final var project = randomFrom(existingProjects);
                builder.remove(project);
            }
        }
        return builder.build();

    }

    private RoutingTable mutate(RoutingTable routingTable) {
        if (routingTable.indicesRouting().isEmpty() || randomBoolean()) {
            return addIndices(randomIntBetween(1, 3), new RoutingTable.Builder(routingTable));
        } else {
            final RoutingTable.Builder builder = new RoutingTable.Builder((routingTable));
            builder.remove(randomFrom(routingTable.indicesRouting().keySet()));
            return builder.build();
        }
    }

    private RoutingTable randomRoutingTable() {
        return addIndices(randomIntBetween(0, 10), new RoutingTable.Builder());
    }

    private static RoutingTable addIndices(int indexCount, RoutingTable.Builder builder) {
        for (int i = 0; i < indexCount; i++) {
            Index index = new Index(randomAlphaOfLengthBetween(3, 24), randomUUID());
            builder.add(IndexRoutingTable.builder(index).build());
        }
        return builder.build();
    }

}
