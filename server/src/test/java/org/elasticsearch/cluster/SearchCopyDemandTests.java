/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchCopyDemandTests extends AbstractWireSerializingTestCase<SearchCopyDemand> {

    @Override
    protected Writeable.Reader<SearchCopyDemand> instanceReader() {
        return SearchCopyDemand::new;
    }

    @Override
    protected SearchCopyDemand createTestInstance() {
        return randomSearchCopyDemand();
    }

    @Override
    protected SearchCopyDemand mutateInstance(SearchCopyDemand instance) {
        if (randomBoolean()) {
            return new SearchCopyDemand(
                randomValueOtherThan(instance.cpuDemand(), SearchCopyDemandTests::randomNonNegativeDouble),
                instance.threadDemand()
            );
        } else {
            return new SearchCopyDemand(
                instance.cpuDemand(),
                randomValueOtherThan(instance.threadDemand(), SearchCopyDemandTests::randomNonNegativeDouble)
            );
        }
    }

    public void testSearchCopyDemandAcceptsZero() {
        final SearchCopyDemand zero = new SearchCopyDemand(0d, 0d);
        assertThat(zero.cpuDemand(), equalTo(0d));
        assertThat(zero.threadDemand(), equalTo(0d));
    }

    public void testRejectsInvalidValues() {
        final double valid = randomNonNegativeDouble();
        for (double rejected : new double[] { Double.NaN, Double.POSITIVE_INFINITY, randomDoubleBetween(Double.NEGATIVE_INFINITY, 0.0d, true) }) {
            final double invalid = rejected;
            final AssertionError cpuError = expectThrows(AssertionError.class, () -> new SearchCopyDemand(invalid, valid));
            assertThat(cpuError.getMessage(), containsString("cpuDemand must be finite and non-negative"));

            final AssertionError threadError = expectThrows(AssertionError.class, () -> new SearchCopyDemand(valid, invalid));
            assertThat(threadError.getMessage(), containsString("threadDemand must be finite and non-negative"));
        }
    }

    private static SearchCopyDemand randomSearchCopyDemand() {
        return new SearchCopyDemand(randomNonNegativeDouble(), randomNonNegativeDouble());
    }

    private static double randomNonNegativeDouble() {
        return randomDouble();
    }
}
