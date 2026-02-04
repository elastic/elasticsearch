/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

public class SamplingConfigurationTests extends AbstractXContentSerializingTestCase<SamplingConfiguration> {

    @Override
    protected SamplingConfiguration doParseInstance(XContentParser parser) throws IOException {
        return SamplingConfiguration.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SamplingConfiguration> instanceReader() {
        return SamplingConfiguration::new;
    }

    @Override
    protected SamplingConfiguration createTestInstance() {
        long maxHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        long maxSizeLimit = (long) (SamplingConfiguration.MAX_SIZE_HEAP_PERCENTAGE_LIMIT * maxHeap);
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, maxSizeLimit)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected SamplingConfiguration mutateInstance(SamplingConfiguration instance) {
        long maxHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        long maxSizeLimit = (long) (SamplingConfiguration.MAX_SIZE_HEAP_PERCENTAGE_LIMIT * maxHeap);
        return switch (randomIntBetween(0, 4)) {
            case 0 -> new SamplingConfiguration(
                randomValueOtherThan(instance.rate(), () -> randomDoubleBetween(0.0, 1.0, true)),
                instance.maxSamples(),
                instance.maxSize(),
                instance.timeToLive(),
                instance.condition()
            );
            case 1 -> new SamplingConfiguration(
                instance.rate(),
                randomValueOtherThan(instance.maxSamples(), () -> randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT)),
                instance.maxSize(),
                instance.timeToLive(),
                instance.condition()
            );
            case 2 -> new SamplingConfiguration(
                instance.rate(),
                instance.maxSamples(),
                randomValueOtherThan(instance.maxSize(), () -> ByteSizeValue.ofBytes(randomLongBetween(1, maxSizeLimit))),
                instance.timeToLive(),
                instance.condition()
            );
            case 3 -> new SamplingConfiguration(
                instance.rate(),
                instance.maxSamples(),
                instance.maxSize(),
                randomValueOtherThan(
                    instance.timeToLive(),
                    () -> TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS))
                ),
                instance.condition()
            );
            case 4 -> new SamplingConfiguration(
                instance.rate(),
                instance.maxSamples(),
                instance.maxSize(),
                instance.timeToLive(),
                randomValueOtherThan(instance.condition(), () -> randomAlphaOfLength(10))
            );
            default -> throw new AssertionError("Unexpected value");
        };
    }

    public void testDefaults() {
        SamplingConfiguration config = new SamplingConfiguration(0.5, null, null, null, null);
        assertThat(config.rate(), equalTo(0.5));
        assertThat(config.maxSamples(), equalTo(SamplingConfiguration.DEFAULT_MAX_SAMPLES));
        long expectedDefaultMaxSize = Math.max(
            (long) (SamplingConfiguration.DEFAULT_MAX_SIZE_HEAP_PERCENTAGE * JvmInfo.jvmInfo().getConfiguredMaxHeapSize()),
            SamplingConfiguration.DEFAULT_MAX_SIZE_FLOOR.getBytes()
        );
        assertThat(config.maxSize(), equalTo(ByteSizeValue.ofBytes(expectedDefaultMaxSize)));
        assertThat(config.timeToLive(), equalTo(TimeValue.timeValueDays(SamplingConfiguration.DEFAULT_TIME_TO_LIVE_DAYS)));
        assertThat(config.condition(), nullValue());
    }

    public void testValidation() throws IOException {
        // Test invalid rate
        assertValidationError("""
            {
              "rate": -0.1
            }
            """, SamplingConfiguration.INVALID_RATE_MESSAGE);

        assertValidationError("""
            {
              "rate": 1.1
            }
            """, SamplingConfiguration.INVALID_RATE_MESSAGE);

        // Test invalid maxSamples
        assertValidationError("""
            {
              "rate": 0.5,
              "max_samples": 0
            }
            """, SamplingConfiguration.INVALID_MAX_SAMPLES_MIN_MESSAGE);

        assertValidationError("""
            {
              "rate": 0.5,
              "max_samples": -1
            }
            """, SamplingConfiguration.INVALID_MAX_SAMPLES_MIN_MESSAGE);

        assertValidationError(String.format(Locale.ROOT, """
            {
              "rate": 0.5,
              "max_samples": %d
            }
            """, SamplingConfiguration.MAX_SAMPLES_LIMIT + 1), SamplingConfiguration.INVALID_MAX_SAMPLES_MAX_MESSAGE);

        // Test invalid maxSize
        assertValidationError("""
            {
              "rate": 0.5,
              "max_size_in_bytes": 0
            }
            """, SamplingConfiguration.INVALID_MAX_SIZE_MIN_MESSAGE);

        assertValidationError("""
            {
              "rate": 0.5,
              "max_size_in_bytes": -1
            }
            """, SamplingConfiguration.INVALID_MAX_SIZE_MIN_MESSAGE);

        // Test max size exceeding heap-based limit
        long maxHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        long maxSizeLimit = (long) (SamplingConfiguration.MAX_SIZE_HEAP_PERCENTAGE_LIMIT * maxHeap);
        assertValidationError(String.format(Locale.ROOT, """
            {
              "rate": 0.5,
              "max_size_in_bytes": %d
            }
            """, maxSizeLimit + 1), SamplingConfiguration.INVALID_MAX_SIZE_MAX_MESSAGE);

        // Test invalid timeToLive
        assertValidationError("""
            {
              "rate": 0.5,
              "time_to_live_in_millis": 0
            }
            """, SamplingConfiguration.INVALID_TIME_TO_LIVE_MIN_MESSAGE);

        assertValidationError("""
            {
              "rate": 0.5,
              "time_to_live": "-1d"
            }
            """, SamplingConfiguration.INVALID_TIME_TO_LIVE_MIN_MESSAGE);

        assertValidationError(String.format(Locale.ROOT, """
            {
              "rate": 0.5,
              "time_to_live": "%dd"
            }
            """, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS + 1), SamplingConfiguration.INVALID_TIME_TO_LIVE_MAX_MESSAGE);

        // Test invalid condition
        assertValidationError("""
            {
              "rate": 0.5,
              "if": ""
            }
            """, SamplingConfiguration.INVALID_CONDITION_MESSAGE);
    }

    // Helper method to find a cause with a specific message in the cause chain
    private Throwable findCauseWithMessage(Throwable throwable, String expectedMessage) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof IllegalArgumentException && expectedMessage.equals(current.getMessage())) {
                return current;
            }
            current = current.getCause();
        }
        return null;
    }

    /**
     * Helper method to test that fromXContentUserData throws validation errors for invalid JSON input
     */
    private void assertValidationError(String jsonInput, String expectedErrorMessage) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, jsonInput);
        Exception e = expectThrows(Exception.class, () -> SamplingConfiguration.fromXContentUserData(parser));
        Throwable cause = findCauseWithMessage(e, expectedErrorMessage);
        assertNotNull("Expected validation error: " + expectedErrorMessage, cause);
        assertThat(cause.getMessage(), equalTo(expectedErrorMessage));
    }

    public void testValidInputs() throws IOException {
        // Test boundary conditions - minimum values
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "rate": 0.001,
              "max_samples": 1,
              "max_size_in_bytes": 1,
              "time_to_live_in_millis": 1,
              "if": "test_condition"
            }
            """);
        SamplingConfiguration config = SamplingConfiguration.fromXContent(parser);
        assertThat(config.rate(), equalTo(0.001));
        assertThat(config.maxSamples(), equalTo(1));

        // Test boundary conditions - maximum values
        long maxHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        long maxSizeLimit = (long) (SamplingConfiguration.MAX_SIZE_HEAP_PERCENTAGE_LIMIT * maxHeap);
        parser = createParser(JsonXContent.jsonXContent, String.format(Locale.ROOT, """
            {
              "rate": 1.0,
              "max_samples": %d,
              "max_size_in_bytes": %d,
              "time_to_live": "%dd",
              "if": "test_condition"
            }
            """, SamplingConfiguration.MAX_SAMPLES_LIMIT, maxSizeLimit, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS));
        config = SamplingConfiguration.fromXContent(parser);
        assertThat(config.rate(), equalTo(1.0));
        assertThat(config.maxSamples(), equalTo(SamplingConfiguration.MAX_SAMPLES_LIMIT));
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return randomBoolean() ? ToXContent.EMPTY_PARAMS : new ToXContent.MapParams(Map.of("human", "true"));
    }

    public void testHumanReadableParsing() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "rate": "0.05",
              "max_samples": 20,
              "max_size": "10mb",
              "time_to_live": "1d",
              "if": "ctx?.network?.name == 'Guest'"
            }
            """);
        SamplingConfiguration configuration = SamplingConfiguration.fromXContent(parser);
        assertThat(configuration.rate(), equalTo(0.05));
        assertThat(configuration.maxSize(), equalTo(ByteSizeValue.ofMb(10)));
        assertThat(configuration.timeToLive(), equalTo(TimeValue.timeValueDays(1)));
    }

    public void testCreationTime() throws IOException {
        // Test that creation time is automatically set when not provided
        long beforeCreation = java.time.Instant.now().toEpochMilli();
        SamplingConfiguration config = new SamplingConfiguration(0.5, null, null, null, null);
        long afterCreation = java.time.Instant.now().toEpochMilli();

        assertThat(config.creationTime(), greaterThanOrEqualTo(beforeCreation));
        assertThat(config.creationTime(), lessThanOrEqualTo(afterCreation));

        // Test that explicit creation time is preserved
        long explicitTime = java.time.Instant.parse("2023-10-05T12:34:56.789Z").toEpochMilli();
        SamplingConfiguration configWithTime = new SamplingConfiguration(0.5, null, null, null, null, explicitTime);
        assertThat(configWithTime.creationTime(), equalTo(explicitTime));
    }

    public void testCreationTimeUserDataRestrictionHumanReadable() throws IOException {
        // Test that user data cannot set creation time via human-readable field
        final XContentParser parserA = createParser(JsonXContent.jsonXContent, """
            {
              "rate": "0.05",
              "creation_time": "2023-10-05T12:34:56.789Z"
            }
            """);
        Exception e = expectThrows(Exception.class, () -> SamplingConfiguration.fromXContentUserData(parserA));
        // The IllegalArgumentException may be wrapped by the parser, so check the cause chain
        Throwable cause = e.getCause();
        while (cause != null
            && (cause instanceof IllegalArgumentException
                && cause.getMessage().equals("Creation time cannot be set by user (field: creation_time)")) == false) {
            cause = cause.getCause();
        }
        assertNotNull("Expected IllegalArgumentException with creation_time message", cause);
        assertThat(cause.getMessage(), equalTo("Creation time cannot be set by user (field: creation_time)"));
    }

    public void testCreationTimeUserDataRestrictionRaw() throws IOException {
        // Test that user data cannot set creation time via machine-readable field
        final XContentParser parserB = createParser(JsonXContent.jsonXContent, """
            {
              "rate": "0.05",
              "creation_time_in_millis": 1696508096789
            }
            """);
        Exception e = expectThrows(Exception.class, () -> SamplingConfiguration.fromXContentUserData(parserB));
        // The IllegalArgumentException may be wrapped by the parser, so check the cause chain
        Throwable cause = e;
        while (cause != null
            && (cause instanceof IllegalArgumentException
                && cause.getMessage().equals("Creation time cannot be set by user (field: creation_time_in_millis)")) == false) {
            cause = cause.getCause();
        }
        assertNotNull("Expected IllegalArgumentException with creation_time_in_millis message", cause);
        assertThat(cause.getMessage(), equalTo("Creation time cannot be set by user (field: creation_time_in_millis)"));
    }

    public void testMinimumDefaultMaxSize() {
        // Test that the minimum default max size is enforced
        SamplingConfiguration config = new SamplingConfiguration(0.5, null, null, null, null);

        // Calculate what the heap percentage would give us
        long heapBasedSize = (long) (SamplingConfiguration.DEFAULT_MAX_SIZE_HEAP_PERCENTAGE * JvmInfo.jvmInfo().getConfiguredMaxHeapSize());
        long minSize = SamplingConfiguration.DEFAULT_MAX_SIZE_FLOOR.getBytes();

        // The actual default should be the larger of the two
        long expectedSize = Math.max(heapBasedSize, minSize);
        assertThat(config.maxSize().getBytes(), equalTo(expectedSize));

        // Verify it's at least the minimum
        assertThat(config.maxSize().getBytes(), greaterThanOrEqualTo(minSize));
    }
}
