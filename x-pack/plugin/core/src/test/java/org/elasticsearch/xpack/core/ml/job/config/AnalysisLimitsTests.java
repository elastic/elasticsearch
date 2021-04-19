/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class AnalysisLimitsTests extends AbstractSerializingTestCase<AnalysisLimits> {

    @Override
    protected AnalysisLimits createTestInstance() {
        return createRandomized();
    }

    public static AnalysisLimits createRandomized() {
        return new AnalysisLimits(randomBoolean() ? (long) randomIntBetween(1, 1000000) : null,
                randomBoolean() ? randomNonNegativeLong() : null);
    }

    @Override
    protected Writeable.Reader<AnalysisLimits> instanceReader() {
        return AnalysisLimits::new;
    }

    @Override
    protected AnalysisLimits doParseInstance(XContentParser parser) {
        return AnalysisLimits.STRICT_PARSER.apply(parser, null);
    }

    public void testParseModelMemoryLimitGivenNegativeNumber() throws IOException {
        String json = "{\"model_memory_limit\": -1}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        XContentParseException e = expectThrows(XContentParseException.class, () -> AnalysisLimits.STRICT_PARSER.apply(parser, null));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("model_memory_limit must be at least 1 MiB. Value = -1"));
    }

    public void testParseModelMemoryLimitGivenZero() throws IOException {
        String json = "{\"model_memory_limit\": 0}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        XContentParseException e = expectThrows(XContentParseException.class, () -> AnalysisLimits.STRICT_PARSER.apply(parser, null));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("model_memory_limit must be at least 1 MiB. Value = 0"));
    }

    public void testParseModelMemoryLimitGivenPositiveNumber() throws IOException {
        String json = "{\"model_memory_limit\": 2048}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);

        AnalysisLimits limits = AnalysisLimits.STRICT_PARSER.apply(parser, null);

        assertThat(limits.getModelMemoryLimit(), equalTo(2048L));
    }

    public void testParseModelMemoryLimitGivenNegativeString() throws IOException {
        String json = "{\"model_memory_limit\":\"-4MB\"}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        XContentParseException e = expectThrows(XContentParseException.class, () -> AnalysisLimits.STRICT_PARSER.apply(parser, null));
        // the root cause is wrapped in an intermediate ElasticsearchParseException
        assertThat(e.getCause(), instanceOf(ElasticsearchParseException.class));
        assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getCause().getMessage(), containsString("Values less than -1 bytes are not supported: -4mb"));
    }

    public void testParseModelMemoryLimitGivenZeroString() throws IOException {
        String json = "{\"model_memory_limit\":\"0MB\"}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        XContentParseException e = expectThrows(XContentParseException.class, () -> AnalysisLimits.STRICT_PARSER.apply(parser, null));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("model_memory_limit must be at least 1 MiB. Value = 0"));
    }

    public void testParseModelMemoryLimitGivenLessThanOneMBString() throws IOException {
        String json = "{\"model_memory_limit\":\"1000Kb\"}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        XContentParseException e = expectThrows(XContentParseException.class, () -> AnalysisLimits.STRICT_PARSER.apply(parser, null));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("model_memory_limit must be at least 1 MiB. Value = 0"));
    }

    public void testParseModelMemoryLimitGivenStringMultipleOfMBs() throws IOException {
        String json = "{\"model_memory_limit\":\"4g\"}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);

        AnalysisLimits limits = AnalysisLimits.STRICT_PARSER.apply(parser, null);

        assertThat(limits.getModelMemoryLimit(), equalTo(4096L));
    }

    public void testParseModelMemoryLimitGivenStringNonMultipleOfMBs() throws IOException {
        String json = "{\"model_memory_limit\":\"1300kb\"}";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);

        AnalysisLimits limits = AnalysisLimits.STRICT_PARSER.apply(parser, null);

        assertThat(limits.getModelMemoryLimit(), equalTo(1L));
    }

    public void testModelMemoryDefault() {
        AnalysisLimits limits = new AnalysisLimits(randomNonNegativeLong());
        assertThat(limits.getModelMemoryLimit(), equalTo(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB));
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
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> new AnalysisLimits(1L, -1L));
        String errorMessage = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW,
                AnalysisLimits.CATEGORIZATION_EXAMPLES_LIMIT, 0, -1L);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenValid() {
        new AnalysisLimits(null, 1L);
        new AnalysisLimits(1L, null);
        new AnalysisLimits(1L, 1L);
    }

    @Override
    protected AnalysisLimits mutateInstance(AnalysisLimits instance) throws IOException {
        Long memoryModelLimit = instance.getModelMemoryLimit();
        Long categorizationExamplesLimit = instance.getCategorizationExamplesLimit();
        switch (between(0, 1)) {
        case 0:
            if (memoryModelLimit == null) {
                memoryModelLimit = randomNonNegativeLong();
            } else {
                if (randomBoolean()) {
                    memoryModelLimit = null;
                } else {
                    memoryModelLimit += between(1, 10000);
                }
            }
            break;
        case 1:
            if (categorizationExamplesLimit == null) {
                categorizationExamplesLimit = randomNonNegativeLong();
            } else {
                if (randomBoolean()) {
                    categorizationExamplesLimit = null;
                } else {
                    categorizationExamplesLimit += between(1, 10000);
                }
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new AnalysisLimits(memoryModelLimit, categorizationExamplesLimit);
    }
}
