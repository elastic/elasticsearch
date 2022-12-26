/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class BlockBuilderTests extends ESTestCase {

    public void testDouble() {
        BlockBuilder builder = BlockBuilder.newDoubleBlockBuilder(0);
        builder.appendNull();
        builder.appendNull();
        Block block = builder.build();

        assertThat(block.getPositionCount(), is(2));
        assertThat(block.isNull(0), is(true));
        assertThat(block.isNull(1), is(true));
    }
}
