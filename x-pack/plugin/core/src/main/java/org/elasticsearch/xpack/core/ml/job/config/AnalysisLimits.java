/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

/**
 * Analysis limits for autodetect. In particular,
 * this is a collection of parameters that allow limiting
 * the resources used by the job.
 */
public class AnalysisLimits implements ToXContentObject, Writeable {

    /**
     * Prior to 6.1 the default model memory size limit was 4GB, and defined in the C++ code.  The default
     * is now 1GB and defined here in the Java code.  Prior to 6.3, a value of <code>null</code> means that
     * the old default value should be used. From 6.3 onwards, the value will always be explicit.
     */
    public static final long DEFAULT_MODEL_MEMORY_LIMIT_MB = 1024L;
    public static final long PRE_6_1_DEFAULT_MODEL_MEMORY_LIMIT_MB = 4096L;

    public static final long DEFAULT_CATEGORIZATION_EXAMPLES_LIMIT = 4;

    /**
     * Serialisation field names
     */
    public static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    public static final ParseField CATEGORIZATION_EXAMPLES_LIMIT = new ParseField("categorization_examples_limit");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<AnalysisLimits, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<AnalysisLimits, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<AnalysisLimits, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<AnalysisLimits, Void> parser = new ConstructingObjectParser<>(
            "analysis_limits",
            ignoreUnknownFields,
            a -> ignoreUnknownFields
                ? new AnalysisLimits(
                    a[0] == null ? PRE_6_1_DEFAULT_MODEL_MEMORY_LIMIT_MB : (Long) a[0],
                    a[1] == null ? DEFAULT_CATEGORIZATION_EXAMPLES_LIMIT : (Long) a[1]
                )
                : new AnalysisLimits((Long) a[0], (Long) a[1])
        );

        parser.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()).getMb();
            } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, MODEL_MEMORY_LIMIT, ObjectParser.ValueType.VALUE);
        parser.declareLong(ConstructingObjectParser.optionalConstructorArg(), CATEGORIZATION_EXAMPLES_LIMIT);

        return parser;
    }

    /**
     * The model memory limit in MiBs.
     * It is initialised to <code>null</code>.
     * A value of <code>null</code> will result to the default defined in the C++ code being used.
     * However, for jobs created in version 6.1 or higher this will rarely be <code>null</code> because
     * the put_job action set it to a new default defined in the Java code.
     */
    private final Long modelMemoryLimit;

    /**
     * It is initialised to <code>null</code>.
     * A value of <code>null</code> will result to the default being used.
     */
    private final Long categorizationExamplesLimit;

    public AnalysisLimits(Long categorizationExamplesLimit) {
        this(DEFAULT_MODEL_MEMORY_LIMIT_MB, categorizationExamplesLimit);
    }

    public AnalysisLimits(Long modelMemoryLimitMb, Long categorizationExamplesLimit) {
        if (modelMemoryLimitMb != null && modelMemoryLimitMb < 1) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_TOO_LOW, modelMemoryLimitMb, "1 MiB");
            throw ExceptionsHelper.badRequestException(msg);
        }
        if (categorizationExamplesLimit != null && categorizationExamplesLimit < 0) {
            String msg = Messages.getMessage(
                Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW,
                CATEGORIZATION_EXAMPLES_LIMIT,
                0,
                categorizationExamplesLimit
            );
            throw ExceptionsHelper.badRequestException(msg);
        }
        this.modelMemoryLimit = modelMemoryLimitMb;
        this.categorizationExamplesLimit = categorizationExamplesLimit;
    }

    public AnalysisLimits(StreamInput in) throws IOException {
        this(in.readOptionalLong(), in.readOptionalLong());
    }

    /**
     * Creates a new {@code AnalysisLimits} object after validating it against external limitations
     * and filling missing values with their defaults. Validations:
     *
     * <ul>
     *   <li>check model memory limit doesn't exceed the MAX_MODEL_MEM setting</li>
     * </ul>
     *
     * @param source an optional {@code AnalysisLimits} whose explicit values will be copied
     * @param maxModelMemoryLimit the max allowed model memory limit
     * @param defaultModelMemoryLimit the default model memory limit to be used if an explicit value is missing
     * @return a new {@code AnalysisLimits} that is validated and has no missing values
     */
    public static AnalysisLimits validateAndSetDefaults(
        @Nullable AnalysisLimits source,
        @Nullable ByteSizeValue maxModelMemoryLimit,
        long defaultModelMemoryLimit
    ) {

        boolean maxModelMemoryIsSet = maxModelMemoryLimit != null && maxModelMemoryLimit.getMb() > 0;

        long modelMemoryLimit = defaultModelMemoryLimit;
        if (maxModelMemoryIsSet) {
            modelMemoryLimit = Math.min(maxModelMemoryLimit.getMb(), modelMemoryLimit);
        }

        long categorizationExamplesLimit = DEFAULT_CATEGORIZATION_EXAMPLES_LIMIT;

        if (source != null) {
            if (source.getModelMemoryLimit() != null) {
                modelMemoryLimit = source.getModelMemoryLimit();
            }
            if (source.getCategorizationExamplesLimit() != null) {
                categorizationExamplesLimit = source.getCategorizationExamplesLimit();
            }
        }

        if (maxModelMemoryIsSet && modelMemoryLimit > maxModelMemoryLimit.getMb()) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_GREATER_THAN_MAX,
                    ByteSizeValue.ofMb(modelMemoryLimit),
                    maxModelMemoryLimit
                )
            );
        }

        return new AnalysisLimits(modelMemoryLimit, categorizationExamplesLimit);
    }

    /**
     * Maximum size of the model in MB before the anomaly detector
     * will drop new samples to prevent the model using any more
     * memory.
     *
     * @return The set memory limit or <code>null</code> if not set
     */
    @Nullable
    public Long getModelMemoryLimit() {
        return modelMemoryLimit;
    }

    /**
     * Gets the limit to the number of examples that are stored per category
     *
     * @return the limit or <code>null</code> if not set
     */
    @Nullable
    public Long getCategorizationExamplesLimit() {
        return categorizationExamplesLimit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalLong(modelMemoryLimit);
        out.writeOptionalLong(categorizationExamplesLimit);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (modelMemoryLimit != null) {
            builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit + "mb");
        }
        if (categorizationExamplesLimit != null) {
            builder.field(CATEGORIZATION_EXAMPLES_LIMIT.getPreferredName(), categorizationExamplesLimit);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Overridden equality test
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof AnalysisLimits == false) {
            return false;
        }

        AnalysisLimits that = (AnalysisLimits) other;
        return Objects.equals(this.modelMemoryLimit, that.modelMemoryLimit)
            && Objects.equals(this.categorizationExamplesLimit, that.categorizationExamplesLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelMemoryLimit, categorizationExamplesLimit);
    }
}
