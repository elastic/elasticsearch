/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
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
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
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
            super(version, routingTable);
        }

        public GlobalRoutingTableWithEquals(GlobalRoutingTable other) {
            super(other.version(), other.routingTables());
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
            if (left.version() != right.version()) {
                return false;
            }
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
            return left.version() == right.version() && Objects.equals(left.indicesRouting(), right.indicesRouting());
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(version());
            // This is not pretty, but it's necessary because ImmutableOpenMap does not guarantee that the iteration order is identical
            // for separate instances with the same elements
            final var iterator = routingTables().entrySet().stream().sorted(Comparator.comparing(e -> e.getKey().id())).iterator();
            while (iterator.hasNext()) {
                final Map.Entry<ProjectId, RoutingTable> entry = iterator.next();
                result = 31 * result + Objects.hash(entry.getKey(), entry.getValue().version(), entry.getValue().indicesRouting());
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
        final Function<GlobalRoutingTable, GlobalRoutingTable> mutator = instance -> {
            if (randomBoolean()) {
                return new GlobalRoutingTable(randomValueOtherThan(instance.version(), ESTestCase::randomLong), instance.routingTables());
            } else {
                return new GlobalRoutingTable(
                    instance.version(),
                    ImmutableOpenMap.builder(transformValues(instance.routingTables(), this::mutate)).build()
                );
            }
        };
        DiffableTestUtils.testDiffableSerialization(
            () -> testRoutingTable(1),
            mutator,
            getNamedWriteableRegistry(),
            instanceReader(),
            GlobalRoutingTable::readDiffFrom,
            PRE_MULTI_PROJECT_TRANSPORT_VERSION,
            (original, reconstructed) -> {
                // The round-trip will lose the version of the global table and replace it with the version from the inner routing table
                return GlobalRoutingTableWithEquals.equals(original.getRoutingTable(), reconstructed.getRoutingTable())
                    && reconstructed.version() == reconstructed.getRoutingTable().version();
            }
        );
    }

    public void testHasSameIndexRouting() {
        final GlobalRoutingTable original = randomValueOtherThanMany(
            grt -> grt.totalIndexCount() == 0, // The mutation below assume that there is at least 1 index
            () -> this.testRoutingTable(randomIntBetween(1, 5))
        );

        // Exactly the same projects => same routing
        GlobalRoutingTable updated = new GlobalRoutingTable(randomLong(), original.routingTables());
        assertTrue(original.hasSameIndexRouting(updated));
        assertTrue(updated.hasSameIndexRouting(original));

        // Updated projects but with same routing => same routing
        updated = new GlobalRoutingTable(
            randomLong(),
            ImmutableOpenMap.builder(transformValues(original.routingTables(), rt -> RoutingTable.builder(rt).incrementVersion().build()))
                .build()
        );
        assertTrue(original.hasSameIndexRouting(updated));
        assertTrue(updated.hasSameIndexRouting(original));

        // Reconstructed index map (with same elements) => different routing
        updated = new GlobalRoutingTable(randomLong(), ImmutableOpenMap.builder(transformValues(original.routingTables(), rt -> {
            final RoutingTable.Builder builder = RoutingTable.builder().version(rt.version());
            rt.indicesRouting().values().forEach(builder::add);
            return builder.build();
        })).build());
        assertFalse(original.hasSameIndexRouting(updated));
        assertFalse(updated.hasSameIndexRouting(original));

        // Mutated routing table => different routing
        updated = new GlobalRoutingTable(original.version(), mutate(original.routingTables()));
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
            () -> randomValueOtherThanMany(table1.routingTables()::containsKey, () -> new ProjectId(randomUUID()))
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
        final long version = randomLong();
        final int numberOfProjects = randomIntBetween(1, 10);
        final ProjectId[] projectIds = randomArray(numberOfProjects, numberOfProjects, ProjectId[]::new, () -> new ProjectId(randomUUID()));
        final Long[] projectVersions = randomArray(numberOfProjects, numberOfProjects, Long[]::new, ESTestCase::randomLong);
        final Integer[] projectIndexCount = randomArray(numberOfProjects, numberOfProjects, Integer[]::new, () -> randomIntBetween(0, 12));

        final GlobalRoutingTable.Builder builder = GlobalRoutingTable.builder();
        builder.version(version);
        for (int i = 0; i < numberOfProjects; i++) {
            builder.put(projectIds[i], addIndices(projectIndexCount[i], RoutingTable.builder().version(projectVersions[i])));
        }
        final GlobalRoutingTable builtRoutingTable = builder.build();

        assertThat(builtRoutingTable.version(), equalTo(version));
        assertThat(builtRoutingTable.size(), equalTo(numberOfProjects));
        assertThat(builtRoutingTable.routingTables().keySet(), containsInAnyOrder(projectIds));
        for (int i = 0; i < numberOfProjects; i++) {
            final ProjectId projectId = projectIds[i];
            assertThat(builtRoutingTable.routingTables(), hasKey(projectId));
            final RoutingTable projectRoutingTable = builtRoutingTable.routingTable(projectId);
            assertThat(projectRoutingTable.version(), equalTo(projectVersions[i]));
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
            final var instance = GlobalRoutingTable.builder(initial).incrementVersion().build();
            assertThat(instance.version(), equalTo(initial.version() + 1));
            assertThat(instance.routingTables(), equalTo(initial.routingTables()));
        }

        {
            final var instance = GlobalRoutingTable.builder(initial).clear().build();
            assertThat(instance.version(), equalTo(initial.version()));
            assertThat(instance.routingTables(), anEmptyMap());
        }

        {
            final ProjectId projectId = randomProjectId();
            final RoutingTable projectRouting = randomRoutingTable();
            final var instance = GlobalRoutingTable.builder(initial).put(projectId, projectRouting).build();
            assertThat(instance.version(), equalTo(initial.version()));
            assertThat(instance.routingTables(), aMapWithSize(initial.size() + 1));
            assertThat(instance.routingTables(), hasEntry(projectId, projectRouting));
            initial.routingTables().forEach((id, rt) -> assertThat(instance.routingTables(), hasEntry(id, rt)));
        }

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
            () -> new Tuple<>(randomProjectId(), randomRoutingTable())
        );
        return new GlobalRoutingTableWithEquals(randomLong(), ImmutableOpenMap.builder(map).build());
    }

    @Override
    protected GlobalRoutingTable mutateInstance(GlobalRoutingTable instance) {
        if (randomBoolean()) {
            return new GlobalRoutingTable(randomValueOtherThan(instance.version(), ESTestCase::randomLong), instance.routingTables());
        } else {
            return new GlobalRoutingTable(instance.version(), mutate(instance.routingTables()));
        }
    }

    private ImmutableOpenMap<ProjectId, RoutingTable> mutate(ImmutableOpenMap<ProjectId, RoutingTable> routingTables) {
        if (routingTables.isEmpty()) {
            return ImmutableOpenMap.builder(Map.of(randomProjectId(), randomRoutingTable())).build();
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
                final var project = randomValueOtherThanMany(existingProjects::contains, GlobalRoutingTableTests::randomProjectId);
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

    private static ProjectId randomProjectId() {
        return new ProjectId(randomUUID());
    }

    private RoutingTable randomRoutingTable() {
        return addIndices(randomIntBetween(0, 10), new RoutingTable.Builder().version(randomLong()));
    }

    private static RoutingTable addIndices(int indexCount, RoutingTable.Builder builder) {
        for (int i = 0; i < indexCount; i++) {
            Index index = new Index(randomAlphaOfLengthBetween(3, 24), randomUUID());
            builder.add(IndexRoutingTable.builder(index).build());
        }
        return builder.build();
    }

}
