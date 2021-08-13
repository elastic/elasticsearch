/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AnalysisLimitsTests extends AbstractXContentTestCase<AnalysisLimits> {

    @Override
    protected AnalysisLimits createTestInstance() {
        return createRandomized();
    }

    public static AnalysisLimits createRandomized() {
        return new AnalysisLimits(randomBoolean() ? (long) randomIntBetween(1, 1000000) : null,
                randomBoolean() ? randomNonNegativeLong() : null);
    }

    @Override
    protected AnalysisLimits doParseInstance(XContentParser parser) {
        return AnalysisLimits.PARSER.apply(parser, null);
    }

    public void testParseModelMemoryLimitGivenPositiveNumber() throws IOException {
        String json = "{\"model_memory_limit\": 2048}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);

        AnalysisLimits limits = AnalysisLimits.PARSER.apply(parser, null);

        assertThat(limits.getModelMemoryLimit(), equalTo(2048L));
    }

    public void testParseModelMemoryLimitGivenStringMultipleOfMBs() throws IOException {
        String json = "{\"model_memory_limit\":\"4g\"}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);

        AnalysisLimits limits = AnalysisLimits.PARSER.apply(parser, null);

        assertThat(limits.getModelMemoryLimit(), equalTo(4096L));
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

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
