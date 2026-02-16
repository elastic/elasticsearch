/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.spi.partitioning.NodeAffinity;

import java.util.OptionalLong;

/**
 * Tests for {@link DataSourcePartition}.
 */
public class DataSourcePartitionTests extends ESTestCase {

    public void testSingleCreatesCoordinatorPartition() {
        DataSourcePartition partition = DataSourcePartition.single(null);
        assertNull(partition.plan());
    }

    public void testCoordinatorPartitionDefaults() {
        DataSourcePartition partition = DataSourcePartition.single(null);
        assertEquals(NodeAffinity.NONE, partition.nodeAffinity());
        assertEquals(OptionalLong.empty(), partition.estimatedRows());
        assertEquals(OptionalLong.empty(), partition.estimatedBytes());
    }

    public void testCoordinatorPartitionWriteToThrows() {
        DataSourcePartition partition = DataSourcePartition.single(null);
        expectThrows(UnsupportedOperationException.class, () -> partition.writeTo(null));
    }

    public void testCoordinatorPartitionWriteableName() {
        DataSourcePartition partition = DataSourcePartition.single(null);
        assertEquals("esql.partition.coordinator", partition.getWriteableName());
    }
}
