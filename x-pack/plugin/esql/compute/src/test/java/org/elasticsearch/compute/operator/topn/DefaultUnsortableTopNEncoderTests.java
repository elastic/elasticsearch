/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DefaultUnsortableTopNEncoderTests extends ESTestCase {
    public void testVIntSmall() {
        testVInt(between(0, 127), 1);
    }

    public void testVIntMed() {
        testVInt(between(128, 16383), 2);
    }

    public void testVIntBig() {
        testVInt(between(16384, 2097151), 3);
    }

    public void testVIntBigger() {
        testVInt(between(2097152, 268435455), 4);
    }

    public void testVIntBiggest() {
        testVInt(between(268435456, Integer.MAX_VALUE), 5);
    }

    public void testVIntNegative() {
        testVInt(between(Integer.MIN_VALUE, -1), 5);
    }

    private void testVInt(int v, int expectedBytes) {
        BreakingBytesRefBuilder builder = ExtractorTests.nonBreakingBytesRefBuilder();
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(v, builder);
        assertThat(builder.length(), equalTo(expectedBytes));
        BytesRef bytes = builder.bytesRefView();
        assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(bytes), equalTo(v));
        assertThat(bytes.length, equalTo(0));
    }
}
