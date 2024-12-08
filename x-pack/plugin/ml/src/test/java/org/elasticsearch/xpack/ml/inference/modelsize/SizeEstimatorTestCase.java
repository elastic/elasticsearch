/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ToXContentObject;

import static org.hamcrest.Matchers.is;

public abstract class SizeEstimatorTestCase<T extends ToXContentObject & Accountable, U extends Accountable> extends
    AbstractXContentTestCase<T> {

    abstract U generateTrueObject();

    abstract T translateObject(U originalObject);

    public void testRamUsageEstimationAccuracy() {
        final long bytesEps = ByteSizeValue.ofKb(2).getBytes();
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            U obj = generateTrueObject();
            T estimateObj = translateObject(obj);
            long originalBytesUsed = obj.ramBytesUsed();
            long estimateBytesUsed = estimateObj.ramBytesUsed();
            // If we are over by 2kb that is small enough to not be a concern
            boolean condition = (Math.abs(obj.ramBytesUsed() - estimateObj.ramBytesUsed()) < bytesEps) ||
            // If the difference is greater than 2kb, it is better to have overestimated.
                originalBytesUsed < estimateBytesUsed;
            assertThat(
                "estimation difference greater than 2048 and the estimation is too small. Object ["
                    + obj.toString()
                    + "] estimated ["
                    + originalBytesUsed
                    + "] translated object ["
                    + estimateObj
                    + "] estimated ["
                    + estimateBytesUsed
                    + "]",
                condition,
                is(true)
            );
        }
    }
}
