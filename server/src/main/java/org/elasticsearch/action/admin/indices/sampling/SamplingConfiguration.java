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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Configuration for sampling raw documents in an index.
 *
 * rate (required): The fraction of documents to sample (between 0 and 1).
 * maxSamples (optional): The maximum number of documents to sample.
 * maxSize (optional): The maximum total size of sampled documents.
 * timeToLive (optional): The duration for which the sampled documents should be retained.
 * condition (optional): An optional condition that sampled documents must satisfy.
 */
public record SamplingConfiguration(double rate, Integer maxSamples, ByteSizeValue maxSize, TimeValue timeToLive, String condition)
    implements
        ToXContentObject,
        SimpleDiffable<SamplingConfiguration> {

    public static final String TYPE = "sampling_configuration";
    private static final String RATE_FIELD_NAME = "rate";
    private static final String MAX_SAMPLES_FIELD_NAME = "max_samples";
    private static final String MAX_SIZE_FIELD_NAME = "max_size";
    private static final String TIME_TO_LIVE_FIELD_NAME = "time_to_live";
    private static final String CONDITION_FIELD_NAME = "condition";

    // Constants for validation and defaults
    public static final int MAX_SAMPLES_LIMIT = 10_000;
    public static final long MAX_SIZE_LIMIT_GIGABYTES = 5;
    public static final long MAX_TIME_TO_LIVE_DAYS = 30;
    public static final int DEFAULT_MAX_SAMPLES = 100;
    public static final long DEFAULT_MAX_SIZE_GIGABYTES = 1;
    public static final long DEFAULT_TIME_TO_LIVE_DAYS = 10;

    // Error messages
    public static final String INVALID_RATE_MESSAGE = "rate must be greater than 0 and less than or equal to 1";
    public static final String INVALID_MAX_SAMPLES_MIN_MESSAGE = "maxSamples must be greater than 0";
    public static final String INVALID_MAX_SAMPLES_MAX_MESSAGE = "maxSamples must be less than or equal to " + MAX_SAMPLES_LIMIT;
    public static final String INVALID_MAX_SIZE_MIN_MESSAGE = "maxSize must be greater than 0";
    public static final String INVALID_MAX_SIZE_MAX_MESSAGE = "maxSize must be less than or equal to " + MAX_SIZE_LIMIT_GIGABYTES + "GB";
    public static final String INVALID_TIME_TO_LIVE_MIN_MESSAGE = "timeToLive must be greater than 0";
    public static final String INVALID_TIME_TO_LIVE_MAX_MESSAGE = "timeToLive must be less than or equal to "
        + MAX_TIME_TO_LIVE_DAYS
        + " days";
    public static final String INVALID_CONDITION_MESSAGE = "condition must be a non-empty string";

    private static final ConstructingObjectParser<SamplingConfiguration, Void> PARSER = new ConstructingObjectParser<>(TYPE, true, args -> {
        double rate = (double) args[0];
        Integer maxSamples = (Integer) args[1];
        ByteSizeValue maxSize = (ByteSizeValue) args[2];
        TimeValue timeToLive = (TimeValue) args[3];
        String condition = (String) args[4];
        return new SamplingConfiguration(rate, maxSamples, maxSize, timeToLive, condition);
    });

    static {
        PARSER.declareDouble(constructorArg(), new ParseField(RATE_FIELD_NAME));
        PARSER.declareIntOrNull(optionalConstructorArg(), DEFAULT_MAX_SAMPLES, new ParseField(MAX_SAMPLES_FIELD_NAME));
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SIZE_FIELD_NAME),
            new org.elasticsearch.xcontent.ParseField(MAX_SIZE_FIELD_NAME),
            org.elasticsearch.xcontent.ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), TIME_TO_LIVE_FIELD_NAME),
            new org.elasticsearch.xcontent.ParseField(TIME_TO_LIVE_FIELD_NAME),
            org.elasticsearch.xcontent.ObjectParser.ValueType.STRING
        );
        PARSER.declareStringOrNull(optionalConstructorArg(), new org.elasticsearch.xcontent.ParseField(CONDITION_FIELD_NAME));
    }

    /*
     * Constructor with validation and defaulting for optional fields.
     */
    public SamplingConfiguration(double rate, Integer maxSamples, ByteSizeValue maxSize, TimeValue timeToLive, String condition) {
        validateInputs(rate, maxSamples, maxSize, timeToLive, condition);

        // Set defaults
        maxSamples = maxSamples == null ? DEFAULT_MAX_SAMPLES : maxSamples;
        maxSize = maxSize == null ? ByteSizeValue.ofGb(DEFAULT_MAX_SIZE_GIGABYTES) : maxSize;
        timeToLive = timeToLive == null ? TimeValue.timeValueDays(DEFAULT_TIME_TO_LIVE_DAYS) : timeToLive;

        // Initialize record fields
        this.rate = rate;
        this.maxSamples = maxSamples;
        this.maxSize = maxSize;
        this.timeToLive = timeToLive;
        this.condition = condition;
    }

    // StreamInput constructor
    public SamplingConfiguration(StreamInput in) throws IOException {
        this(
            in.readDouble(),
            in.readOptionalInt(),
            in.readOptionalWriteable(ByteSizeValue::readFrom),
            in.readOptionalTimeValue(),
            in.readOptionalString()
        );
    }

    // Write to StreamOutput
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(this.rate);
        out.writeOptionalInt(this.maxSamples);
        out.writeOptionalWriteable(this.maxSize);
        out.writeOptionalTimeValue(this.timeToLive);
        out.writeOptionalString(this.condition);
    }

    // Serialize to XContent (JSON)
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RATE_FIELD_NAME, rate);
        if (maxSamples != null) {
            builder.field(MAX_SAMPLES_FIELD_NAME, maxSamples);
        }
        if (maxSize != null) {
            builder.field(MAX_SIZE_FIELD_NAME, maxSize.toString());
        }
        if (timeToLive != null) {
            builder.field(TIME_TO_LIVE_FIELD_NAME, timeToLive.toString());
        }
        if (condition != null && condition.isEmpty() == false) {
            builder.field(CONDITION_FIELD_NAME, condition);
        }
        builder.endObject();
        return builder;
    }

    // Deserialize from XContent (JSON)
    public static SamplingConfiguration fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

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
            ByteSizeValue maxLimit = ByteSizeValue.ofGb(MAX_SIZE_LIMIT_GIGABYTES);
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

        // Validate condition
        if (condition != null && condition.isEmpty()) {
            throw new IllegalArgumentException(INVALID_CONDITION_MESSAGE);
        }
    }
}
