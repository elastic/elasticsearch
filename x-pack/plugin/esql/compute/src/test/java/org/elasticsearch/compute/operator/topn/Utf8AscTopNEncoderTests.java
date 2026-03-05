/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import static org.hamcrest.Matchers.lessThan;

public class Utf8AscTopNEncoderTests extends AbstractUtf8TopNEncoderTests {
    public Utf8AscTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    @Override
    protected TopNEncoder encoder() {
        return TopNEncoder.UTF8;
    }

    @Override
    protected void assertMinMax(BreakingBytesRefBuilder min, BreakingBytesRefBuilder max) {
        assertThat(min.bytesRefView(), lessThan(max.bytesRefView()));
    }
}
