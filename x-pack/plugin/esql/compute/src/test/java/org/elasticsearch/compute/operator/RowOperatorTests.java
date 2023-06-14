/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RowOperatorTests extends ESTestCase {
    final DriverContext driverContext = new DriverContext();

    public void testBoolean() {
        RowOperator.RowOperatorFactory factory = new RowOperator.RowOperatorFactory(List.of(false));
        assertThat(factory.describe(), equalTo("RowOperator[objects = false]"));
        assertThat(factory.get(driverContext).toString(), equalTo("RowOperator[objects=[false]]"));
        BooleanBlock block = factory.get(driverContext).getOutput().getBlock(0);
        assertThat(block.getBoolean(0), equalTo(false));
    }

    public void testInt() {
        RowOperator.RowOperatorFactory factory = new RowOperator.RowOperatorFactory(List.of(213));
        assertThat(factory.describe(), equalTo("RowOperator[objects = 213]"));
        assertThat(factory.get(driverContext).toString(), equalTo("RowOperator[objects=[213]]"));
        IntBlock block = factory.get(driverContext).getOutput().getBlock(0);
        assertThat(block.getInt(0), equalTo(213));
    }

    public void testLong() {
        RowOperator.RowOperatorFactory factory = new RowOperator.RowOperatorFactory(List.of(21321343214L));
        assertThat(factory.describe(), equalTo("RowOperator[objects = 21321343214]"));
        assertThat(factory.get(driverContext).toString(), equalTo("RowOperator[objects=[21321343214]]"));
        LongBlock block = factory.get(driverContext).getOutput().getBlock(0);
        assertThat(block.getLong(0), equalTo(21321343214L));
    }

    public void testDouble() {
        RowOperator.RowOperatorFactory factory = new RowOperator.RowOperatorFactory(List.of(2.0));
        assertThat(factory.describe(), equalTo("RowOperator[objects = 2.0]"));
        assertThat(factory.get(driverContext).toString(), equalTo("RowOperator[objects=[2.0]]"));
        DoubleBlock block = factory.get(driverContext).getOutput().getBlock(0);
        assertThat(block.getDouble(0), equalTo(2.0));
    }

    public void testString() {
        RowOperator.RowOperatorFactory factory = new RowOperator.RowOperatorFactory(List.of(new BytesRef("cat")));
        assertThat(factory.describe(), equalTo("RowOperator[objects = [63 61 74]]"));
        assertThat(factory.get(driverContext).toString(), equalTo("RowOperator[objects=[[63 61 74]]]"));
        BytesRefBlock block = factory.get(driverContext).getOutput().getBlock(0);
        assertThat(block.getBytesRef(0, new BytesRef()), equalTo(new BytesRef("cat")));
    }

    public void testNull() {
        RowOperator.RowOperatorFactory factory = new RowOperator.RowOperatorFactory(Arrays.asList(new Object[] { null }));
        assertThat(factory.describe(), equalTo("RowOperator[objects = null]"));
        assertThat(factory.get(driverContext).toString(), equalTo("RowOperator[objects=[null]]"));
        Block block = factory.get(driverContext).getOutput().getBlock(0);
        assertTrue(block.isNull(0));
    }
}
