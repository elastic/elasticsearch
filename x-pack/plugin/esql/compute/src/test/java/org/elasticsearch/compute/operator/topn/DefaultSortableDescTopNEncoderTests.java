/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import static org.hamcrest.Matchers.greaterThan;

public class DefaultSortableDescTopNEncoderTests extends AbstractDefaultSortableTopNEncoderTests {
    public DefaultSortableDescTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    @Override
    protected TopNEncoder encoder() {
        return TopNEncoder.DEFAULT_SORTABLE.toSortable(false);
    }

    @Override
    protected void assertMinMax(BreakingBytesRefBuilder min, BreakingBytesRefBuilder max) {
        assertThat(min.bytesRefView(), greaterThan(max.bytesRefView()));
    }
}
