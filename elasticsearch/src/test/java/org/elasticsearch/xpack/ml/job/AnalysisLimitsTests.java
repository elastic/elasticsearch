/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class AnalysisLimitsTests extends AbstractSerializingTestCase<AnalysisLimits> {

    @Override
    protected AnalysisLimits createTestInstance() {
        return new AnalysisLimits(randomBoolean() ? randomLong() : null, randomBoolean() ? randomNonNegativeLong() : null);
    }

    @Override
    protected Writeable.Reader<AnalysisLimits> instanceReader() {
        return AnalysisLimits::new;
    }

    @Override
    protected AnalysisLimits parseInstance(XContentParser parser) {
        return AnalysisLimits.PARSER.apply(parser, null);
    }

    public void testEquals_GivenEqual() {
        AnalysisLimits analysisLimits1 = new AnalysisLimits(10L, 20L);
        AnalysisLimits analysisLimits2 = new AnalysisLimits(10L, 20L);

        assertTrue(analysisLimits1.equals(analysisLimits1));
        assertTrue(analysisLimits1.equals(analysisLimits2));
        assertTrue(analysisLimits2.equals(analysisLimits1));
    }


    public void testEquals_GivenDifferentModelMemoryLimit() {
        AnalysisLimits analysisLimits1 = new AnalysisLimits(10L, 20L);
        AnalysisLimits analysisLimits2 = new AnalysisLimits(11L, 20L);

        assertFalse(analysisLimits1.equals(analysisLimits2));
        assertFalse(analysisLimits2.equals(analysisLimits1));
    }


    public void testEquals_GivenDifferentCategorizationExamplesLimit() {
        AnalysisLimits analysisLimits1 = new AnalysisLimits(10L, 20L);
        AnalysisLimits analysisLimits2 = new AnalysisLimits(10L, 21L);

        assertFalse(analysisLimits1.equals(analysisLimits2));
        assertFalse(analysisLimits2.equals(analysisLimits1));
    }


    public void testHashCode_GivenEqual() {
        AnalysisLimits analysisLimits1 = new AnalysisLimits(5555L, 3L);
        AnalysisLimits analysisLimits2 = new AnalysisLimits(5555L, 3L);

        assertEquals(analysisLimits1.hashCode(), analysisLimits2.hashCode());
    }

    public void testVerify_GivenNegativeCategorizationExamplesLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new AnalysisLimits(1L, -1L));
        String errorMessage = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW,
                AnalysisLimits.CATEGORIZATION_EXAMPLES_LIMIT, 0, -1L);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenValid() {
        new AnalysisLimits(0L, 0L);
        new AnalysisLimits(1L, null);
        new AnalysisLimits(1L, 1L);
    }
}
