/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytes;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
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
        var breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
        var recycler = new MockPageCacheRecycler(Settings.EMPTY);
        try (PagedBytesBuilder builder = new PagedBytesBuilder(recycler, breaker, "topn", 0)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(v, builder);
            assertThat(builder.length(), equalTo(expectedBytes));
            try (PagedBytes ref = builder.build()) {
                PagedBytesCursor cursor = ref.cursor(new PagedBytesCursor());
                assertThat(TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(cursor), equalTo(v));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }
}
