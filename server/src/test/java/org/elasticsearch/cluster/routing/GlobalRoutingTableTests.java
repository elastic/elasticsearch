/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.DiffableTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.Objects;
import java.util.function.Function;

public class GlobalRoutingTableTests extends AbstractWireSerializingTestCase<GlobalRoutingTable> {

    /**
     * We intentionally don't want production code comparing two routing tables for equality.
     * But our unit testing frameworks assume that serialized object can be tested for equality
     */
    public static class GlobalRoutingTableWithEquals extends GlobalRoutingTable {

        public GlobalRoutingTableWithEquals(long version, RoutingTable routingTable) {
            super(version, routingTable);
        }

        public GlobalRoutingTableWithEquals(GlobalRoutingTable other) {
            super(other.version(), other.getRoutingTable());
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
            var leftTable = left.getRoutingTable();
            var rightTable = right.getRoutingTable();

            return leftTable.version() == rightTable.version() && Objects.equals(leftTable.indicesRouting(), rightTable.indicesRouting());
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.version(), this.getRoutingTable().version(), this.getRoutingTable().indicesRouting());
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
            this::testRoutingTable,
            this::mutateInstance,
            getNamedWriteableRegistry(),
            instanceReader(),
            GlobalRoutingTable::readDiffFrom,
            GlobalRoutingTableWithEquals::equals
        );
    }

    public void testEmptyDiff() throws Exception {
        DiffableTestUtils.testDiffableSerialization(
            this::testRoutingTable,
            Function.identity(),
            getNamedWriteableRegistry(),
            instanceReader(),
            GlobalRoutingTable::readDiffFrom,
            GlobalRoutingTableWithEquals::equals
        );
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
        return testRoutingTable();
    }

    private GlobalRoutingTable testRoutingTable() {
        return new GlobalRoutingTableWithEquals(randomLong(), randomRoutingTable());
    }

    @Override
    protected GlobalRoutingTable mutateInstance(GlobalRoutingTable instance) {
        if (randomBoolean()) {
            return new GlobalRoutingTable(randomValueOtherThan(instance.version(), ESTestCase::randomLong), instance.getRoutingTable());
        } else {
            return new GlobalRoutingTable(instance.version(), mutate(instance.getRoutingTable()));
        }
    }

    private RoutingTable mutate(RoutingTable routingTable) {
        if (routingTable.indicesRouting().size() == 0 || randomBoolean()) {
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
