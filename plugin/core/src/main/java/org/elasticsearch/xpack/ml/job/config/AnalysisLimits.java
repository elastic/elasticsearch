/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.MlParserType;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Analysis limits for autodetect
 * <p>
 * If an option has not been set it shouldn't be used so the default value is picked up instead.
 */
public class AnalysisLimits implements ToXContentObject, Writeable {

    /**
     * Prior to 6.1 the default model memory size limit was 4GB, and defined in the C++ code.  The default
     * is now 1GB and defined here in the Java code.  However, changing the meaning of a null model memory
     * limit for existing jobs would be a breaking change, so instead the meaning of <code>null</code> is
     * still to use the default from the C++ code, but newly created jobs will have this explicit setting
     * added if none is provided.
     */
    static final long DEFAULT_MODEL_MEMORY_LIMIT_MB = 1024L;

    /**
     * Serialisation field names
     */
    public static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    public static final ParseField CATEGORIZATION_EXAMPLES_LIMIT = new ParseField("categorization_examples_limit");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<AnalysisLimits, Void> METADATA_PARSER = new ConstructingObjectParser<>(
            "analysis_limits", true, a -> new AnalysisLimits((Long) a[0], (Long) a[1]));
    public static final ConstructingObjectParser<AnalysisLimits, Void> CONFIG_PARSER = new ConstructingObjectParser<>(
            "analysis_limits", false, a -> new AnalysisLimits((Long) a[0], (Long) a[1]));
    public static final Map<MlParserType, ConstructingObjectParser<AnalysisLimits, Void>> PARSERS =
            new EnumMap<>(MlParserType.class);

    static {
        PARSERS.put(MlParserType.METADATA, METADATA_PARSER);
        PARSERS.put(MlParserType.CONFIG, CONFIG_PARSER);
        for (MlParserType parserType : MlParserType.values()) {
            ConstructingObjectParser<AnalysisLimits, Void> parser = PARSERS.get(parserType);
            assert parser != null;
            parser.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()).getMb();
                } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.longValue();
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, MODEL_MEMORY_LIMIT, ObjectParser.ValueType.VALUE);
            parser.declareLong(ConstructingObjectParser.optionalConstructorArg(), CATEGORIZATION_EXAMPLES_LIMIT);
        }
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

    public AnalysisLimits(Long modelMemoryLimit, Long categorizationExamplesLimit) {
        if (modelMemoryLimit != null && modelMemoryLimit < 1) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_TOO_LOW, modelMemoryLimit);
            throw ExceptionsHelper.badRequestException(msg);
        }
        if (categorizationExamplesLimit != null && categorizationExamplesLimit < 0) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW, CATEGORIZATION_EXAMPLES_LIMIT, 0,
                    categorizationExamplesLimit);
            throw ExceptionsHelper.badRequestException(msg);
        }
        this.modelMemoryLimit = modelMemoryLimit;
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
        return Objects.equals(this.modelMemoryLimit, that.modelMemoryLimit) &&
                Objects.equals(this.categorizationExamplesLimit, that.categorizationExamplesLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelMemoryLimit, categorizationExamplesLimit);
    }
}
