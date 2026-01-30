/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Configuration for sampling raw documents in an index.
 */
public record SamplingConfiguration(
    double rate,
    Integer maxSamples,
    ByteSizeValue maxSize,
    TimeValue timeToLive,
    String condition,
    Long creationTime
) implements ToXContentObject, SimpleDiffable<SamplingConfiguration> {

    public static final String TYPE = "sampling_configuration";
    private static final String RATE_FIELD_NAME = "rate";
    private static final String MAX_SAMPLES_FIELD_NAME = "max_samples";
    private static final String MAX_SIZE_IN_BYTES_FIELD_NAME = "max_size_in_bytes";
    private static final String MAX_SIZE_FIELD_NAME = "max_size";
    private static final String TIME_TO_LIVE_IN_MILLIS_FIELD_NAME = "time_to_live_in_millis";
    private static final String TIME_TO_LIVE_FIELD_NAME = "time_to_live";
    private static final String CONDITION_FIELD_NAME = "if";
    private static final String CREATION_TIME_IN_MILLIS_FIELD_NAME = "creation_time_in_millis";
    private static final String CREATION_TIME_FIELD_NAME = "creation_time";

    private static final String IS_USER_DATA_CONTEXT_KEY = "is_user_data";

    // Constants for validation and defaults
    public static final int MAX_SAMPLES_LIMIT = 10_000;
    public static final double DEFAULT_MAX_SIZE_HEAP_PERCENTAGE = 0.01;
    public static final double MAX_SIZE_HEAP_PERCENTAGE_LIMIT = 0.05;
    public static final ByteSizeValue DEFAULT_MAX_SIZE_FLOOR = ByteSizeValue.ofKb(100);
    public static final long MAX_TIME_TO_LIVE_DAYS = 30;
    public static final int DEFAULT_MAX_SAMPLES = 100;
    public static final long DEFAULT_TIME_TO_LIVE_DAYS = 10;

    // Error messages
    public static final String INVALID_RATE_MESSAGE = "rate must be greater than 0 and less than or equal to 1";
    public static final String INVALID_MAX_SAMPLES_MIN_MESSAGE = "maxSamples must be greater than 0";
    public static final String INVALID_MAX_SAMPLES_MAX_MESSAGE = "maxSamples must be less than or equal to " + MAX_SAMPLES_LIMIT;
    public static final String INVALID_MAX_SIZE_MIN_MESSAGE = "maxSize must be greater than 0";
    public static final String INVALID_MAX_SIZE_MAX_MESSAGE = "maxSize must be less than or equal to "
        + (int) (MAX_SIZE_HEAP_PERCENTAGE_LIMIT * 100)
        + "% of heap size ("
        + calculateMaxSizeLimit().toString()
        + ")";
    public static final String INVALID_TIME_TO_LIVE_MIN_MESSAGE = "timeToLive must be greater than 0";
    public static final String INVALID_TIME_TO_LIVE_MAX_MESSAGE = "timeToLive must be less than or equal to "
        + MAX_TIME_TO_LIVE_DAYS
        + " days";
    public static final String INVALID_CONDITION_MESSAGE = "condition script, if provided, must not be empty";

    private static final ConstructingObjectParser<SamplingConfiguration, Map<String, Boolean>> PARSER = new ConstructingObjectParser<
        SamplingConfiguration,
        Map<String, Boolean>>(TYPE, false, (args, context) -> {
            Double rate = (Double) args[0];
            Integer maxSamples = (Integer) args[1];
            ByteSizeValue humanReadableMaxSize = (ByteSizeValue) args[2];
            ByteSizeValue rawMaxSize = (ByteSizeValue) args[3];
            TimeValue humanReadableTimeToLive = (TimeValue) args[4];
            TimeValue rawTimeToLive = (TimeValue) args[5];
            String condition = (String) args[6];
            Long rawCreationTime = (Long) args[8];

            if (context.get(IS_USER_DATA_CONTEXT_KEY)) {
                validateInputs(
                    rate,
                    maxSamples,
                    determineValue(humanReadableMaxSize, rawMaxSize),
                    determineValue(humanReadableTimeToLive, rawTimeToLive),
                    condition
                );
            }

            return new SamplingConfiguration(
                rate,
                maxSamples,
                determineValue(humanReadableMaxSize, rawMaxSize),
                determineValue(humanReadableTimeToLive, rawTimeToLive),
                condition,
                rawCreationTime
            );
        });

    static {
        PARSER.declareDouble(constructorArg(), new ParseField(RATE_FIELD_NAME));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(MAX_SAMPLES_FIELD_NAME));
        // Handle both human-readable and machine-readable fields for maxSize.
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> { return ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SIZE_FIELD_NAME); },
            new ParseField(MAX_SIZE_FIELD_NAME),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> { return ByteSizeValue.ofBytes(p.longValue()); },
            new ParseField(MAX_SIZE_IN_BYTES_FIELD_NAME),
            ObjectParser.ValueType.LONG
        );
        // Handle both human-readable and machine-readable fields for timeToLive
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> { return TimeValue.parseTimeValue(p.text(), TIME_TO_LIVE_FIELD_NAME); },
            new ParseField(TIME_TO_LIVE_FIELD_NAME),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> { return TimeValue.timeValueMillis(p.longValue()); },
            new ParseField(TIME_TO_LIVE_IN_MILLIS_FIELD_NAME),
            ObjectParser.ValueType.LONG
        );
        PARSER.declareString(optionalConstructorArg(), new ParseField(CONDITION_FIELD_NAME));
        PARSER.declareField(optionalConstructorArg(), (p, c) -> {
            validateUserDataContext(c, CREATION_TIME_FIELD_NAME);
            return Instant.parse(p.text()).toEpochMilli();
        }, new ParseField(CREATION_TIME_FIELD_NAME), ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> {
            validateUserDataContext(c, CREATION_TIME_IN_MILLIS_FIELD_NAME);
            return p.longValue();
        }, new ParseField(CREATION_TIME_IN_MILLIS_FIELD_NAME), ObjectParser.ValueType.LONG);
    }

    /**
     * Constructor with defaulting for optional fields.
     *
     * @param rate The fraction of documents to sample (must be between 0 and 1)
     * @param maxSamples The maximum number of documents to sample (optional, defaults to {@link #DEFAULT_MAX_SAMPLES})
     * @param maxSize The maximum total size of sampled documents (optional, defaults to {@link #DEFAULT_MAX_SIZE_HEAP_PERCENTAGE} of heap)
     * @param timeToLive The duration for which the sampled documents
     *                   should be retained (optional, defaults to {@link #DEFAULT_TIME_TO_LIVE_DAYS} days)
     * @param condition An optional condition script that sampled documents must satisfy (optional, can be null)
     * @throws IllegalArgumentException If any of the parameters are invalid, according to the validation rules
     */
    public SamplingConfiguration(
        double rate,
        Integer maxSamples,
        ByteSizeValue maxSize,
        TimeValue timeToLive,
        String condition,
        Long creationTime
    ) {
        this.rate = rate;
        this.maxSamples = maxSamples == null ? DEFAULT_MAX_SAMPLES : maxSamples;
        this.maxSize = maxSize == null ? calculateDefaultMaxSize() : maxSize;
        this.timeToLive = timeToLive == null ? TimeValue.timeValueDays(DEFAULT_TIME_TO_LIVE_DAYS) : timeToLive;
        this.condition = condition;
        this.creationTime = creationTime == null ? Instant.now().toEpochMilli() : creationTime;
    }

    /**
     * Calculates the default max size as a percentage of the configured heap size,
     * with a minimum floor value.
     *
     * @return The default max size value
     */
    private static ByteSizeValue calculateDefaultMaxSize() {
        long heapBasedSize = (long) (DEFAULT_MAX_SIZE_HEAP_PERCENTAGE * JvmInfo.jvmInfo().getConfiguredMaxHeapSize());
        return ByteSizeValue.ofBytes(Math.max(heapBasedSize, DEFAULT_MAX_SIZE_FLOOR.getBytes()));
    }

    /**
     * Calculates the max size limit as a percentage of the configured heap size.
     *
     * @return The max size limit value
     */
    public static ByteSizeValue calculateMaxSizeLimit() {
        return ByteSizeValue.ofBytes((long) (MAX_SIZE_HEAP_PERCENTAGE_LIMIT * JvmInfo.jvmInfo().getConfiguredMaxHeapSize()));
    }

    // Convenience constructor without creationTime
    public SamplingConfiguration(double rate, Integer maxSamples, ByteSizeValue maxSize, TimeValue timeToLive, String condition) {
        this(rate, maxSamples, maxSize, timeToLive, condition, null);
    }

    /**
     * Constructs a SamplingConfiguration from a StreamInput for wire protocol deserialization.
     *
     * @param in The StreamInput to read from
     * @throws IOException If an I/O error occurs during deserialization
     */
    public SamplingConfiguration(StreamInput in) throws IOException {
        this(in.readDouble(), in.readInt(), ByteSizeValue.readFrom(in), in.readTimeValue(), in.readOptionalString(), in.readLong());
    }

    // Write to StreamOutput
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(this.rate);
        out.writeInt(this.maxSamples);
        out.writeWriteable(this.maxSize);
        out.writeTimeValue(this.timeToLive);
        out.writeOptionalString(this.condition);
        out.writeLong(this.creationTime);
    }

    // Serialize to XContent (JSON)
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RATE_FIELD_NAME, rate);
        builder.field(MAX_SAMPLES_FIELD_NAME, maxSamples);
        builder.humanReadableField(MAX_SIZE_IN_BYTES_FIELD_NAME, MAX_SIZE_FIELD_NAME, maxSize);
        builder.humanReadableField(TIME_TO_LIVE_IN_MILLIS_FIELD_NAME, TIME_TO_LIVE_FIELD_NAME, timeToLive);
        if (condition != null) {
            builder.field(CONDITION_FIELD_NAME, condition);
        }
        builder.timestampFieldsFromUnixEpochMillis(CREATION_TIME_IN_MILLIS_FIELD_NAME, CREATION_TIME_FIELD_NAME, creationTime);
        builder.endObject();
        return builder;
    }

    /**
     * Parses a SamplingConfiguration from XContent (JSON).
     *
     * @param parser The XContentParser to parse from
     * @return The parsed SamplingConfiguration object
     * @throws IOException If parsing fails due to invalid JSON or I/O errors
     */
    public static SamplingConfiguration fromXContent(XContentParser parser) throws IOException {
        Map<String, Boolean> context = new HashMap<>();
        context.put(IS_USER_DATA_CONTEXT_KEY, false);
        return PARSER.parse(parser, context);
    }

    public static SamplingConfiguration fromXContentUserData(XContentParser parser) throws IOException {
        return PARSER.parse(parser, Map.of(IS_USER_DATA_CONTEXT_KEY, true));
    }

    /**
     * Creates a diff reader for SamplingConfiguration objects that can deserialize diffs from wire protocol.
     *
     * @param in The StreamInput to read the diff from
     * @return A Diff that can be applied to produce the target SamplingConfiguration
     * @throws IOException If an I/O error occurs during deserialization
     */
    public static Diff<SamplingConfiguration> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(SamplingConfiguration::new, in);
    }

    // Input validation method
    private static void validateInputs(double rate, Integer maxSamples, ByteSizeValue maxSize, TimeValue timeToLive, String condition) {
        // Validate rate
        if (rate <= 0 || rate > 1) {
            throw new IllegalArgumentException(INVALID_RATE_MESSAGE);
        }

        // Validate maxSamples
        if (maxSamples != null) {
            if (maxSamples <= 0) {
                throw new IllegalArgumentException(INVALID_MAX_SAMPLES_MIN_MESSAGE);
            }
            if (maxSamples > MAX_SAMPLES_LIMIT) {
                throw new IllegalArgumentException(INVALID_MAX_SAMPLES_MAX_MESSAGE);
            }
        }

        // Validate maxSize
        if (maxSize != null) {
            if (maxSize.compareTo(ByteSizeValue.ZERO) <= 0) {
                throw new IllegalArgumentException(INVALID_MAX_SIZE_MIN_MESSAGE);
            }
            ByteSizeValue maxLimit = calculateMaxSizeLimit();
            if (maxSize.compareTo(maxLimit) > 0) {
                throw new IllegalArgumentException(INVALID_MAX_SIZE_MAX_MESSAGE);
            }
        }

        // Validate timeToLive
        if (timeToLive != null) {
            if (timeToLive.compareTo(TimeValue.ZERO) <= 0) {
                throw new IllegalArgumentException(INVALID_TIME_TO_LIVE_MIN_MESSAGE);
            }
            TimeValue maxLimit = TimeValue.timeValueDays(MAX_TIME_TO_LIVE_DAYS);
            if (timeToLive.compareTo(maxLimit) > 0) {
                throw new IllegalArgumentException(INVALID_TIME_TO_LIVE_MAX_MESSAGE);
            }
        }

        if (condition != null && condition.isEmpty()) {
            throw new IllegalArgumentException(INVALID_CONDITION_MESSAGE);
        }
    }

    private static <T> T determineValue(T humanReadableValue, T rawValue) {
        // If both human-readable and raw fields are present, the human-readable one takes precedence.
        if (humanReadableValue == null && rawValue == null) {
            return null;
        }
        return humanReadableValue != null ? humanReadableValue : rawValue;

    }

    private static void validateUserDataContext(Map<String, Boolean> context, String fieldName) {
        if (context.get(IS_USER_DATA_CONTEXT_KEY) == Boolean.TRUE) {
            throw new IllegalArgumentException("Creation time cannot be set by user (field: " + fieldName + ")");
        }
    }
}
