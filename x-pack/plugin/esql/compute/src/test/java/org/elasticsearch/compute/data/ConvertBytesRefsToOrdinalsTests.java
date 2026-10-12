/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ConvertBytesRefsToOrdinalsTests extends ComputeTestCase {
    /**
     * Enough distinct values that two of them almost surely share a 32-bit {@link BytesRef#hashCode()}
     * (the chance of no collision is about {@code e^-10}), so a dedupe map that mixes up colliding keys fails here.
     */
    public void testPreservesValuesAcrossHashCollisions() {
        int count = 300_000;
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(count)) {
            for (int i = 0; i < count; i++) {
                builder.appendBytesRef(new BytesRef(Integer.toString(i)));
            }
            Page page = BlockTestUtils.convertBytesRefsToOrdinals(new Page(builder.build()));
            try {
                BytesRefBlock converted = page.getBlock(0);
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < count; i++) {
                    assertThat(converted.getBytesRef(i, scratch).utf8ToString(), equalTo(Integer.toString(i)));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }
}
