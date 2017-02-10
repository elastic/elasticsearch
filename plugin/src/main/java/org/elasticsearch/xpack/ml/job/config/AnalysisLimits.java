/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Objects;

/**
 * Analysis limits for autodetect
 * <p>
 * If an option has not been set it shouldn't be used so the default value is picked up instead.
 */
public class AnalysisLimits extends ToXContentToBytes implements Writeable {
    /**
     * Serialisation field names
     */
    public static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    public static final ParseField CATEGORIZATION_EXAMPLES_LIMIT = new ParseField("categorization_examples_limit");

    public static final ConstructingObjectParser<AnalysisLimits, Void> PARSER = new ConstructingObjectParser<>(
            "analysis_limits", a -> new AnalysisLimits((Long) a[0], (Long) a[1]));

    static {
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MODEL_MEMORY_LIMIT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CATEGORIZATION_EXAMPLES_LIMIT);
    }

    /**
     * It is initialised to <code>null</code>.
     * A value of <code>null</code> or <code>0</code> will result to the default being used.
     */
    private final Long modelMemoryLimit;

    /**
     * It is initialised to <code>null</code>.
     * A value of <code>null</code> will result to the default being used.
     */
    private final Long categorizationExamplesLimit;

    public AnalysisLimits(Long modelMemoryLimit, Long categorizationExamplesLimit) {
        this.modelMemoryLimit = modelMemoryLimit;
        if (categorizationExamplesLimit != null && categorizationExamplesLimit < 0) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW, CATEGORIZATION_EXAMPLES_LIMIT, 0,
                    categorizationExamplesLimit);
            throw new IllegalArgumentException(msg);
        }
        this.categorizationExamplesLimit = categorizationExamplesLimit;
    }

    public AnalysisLimits(StreamInput in) throws IOException {
        this(in.readOptionalLong(), in.readOptionalLong());
    }

    /**
     * Maximum size of the model in MB before the anomaly detector
     * will drop new samples to prevent the model using any more
     * memory
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
            builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit);
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
        return Objects.equals(this.modelMemoryLimit, that.modelMemoryLimit) &&
                Objects.equals(this.categorizationExamplesLimit, that.categorizationExamplesLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelMemoryLimit, categorizationExamplesLimit);
    }
}
