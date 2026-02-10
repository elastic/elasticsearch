/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class FixedLengthDescTopNEncoderTests extends AbstractFixedTopNEncoderTests {
    public FixedLengthDescTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    @Override
    protected TopNEncoder encoder() {
        return SortableTopNEncoder.IP.toSortable(false);
    }

    @Override
    protected void assertMinMax(BreakingBytesRefBuilder min, BreakingBytesRefBuilder max) {
        assertThat(min.bytesRefView(), greaterThan(max.bytesRefView()));
    }
}
