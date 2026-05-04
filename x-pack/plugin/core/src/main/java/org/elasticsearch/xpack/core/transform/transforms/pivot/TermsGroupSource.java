/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/*
 * A terms aggregation source for group_by
 */
public class TermsGroupSource extends SingleGroupSource {
    private static final String NAME = "data_frame_terms_group";

    public static final TransportVersion MAX_TERMS_FOR_CHANGE_DETECTION = TransportVersion.fromName(
        "transform_max_terms_for_change_detection"
    );

    private static final ConstructingObjectParser<TermsGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TermsGroupSource, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TermsGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TermsGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            ScriptConfig scriptConfig = (ScriptConfig) args[1];
            boolean missingBucket = args[2] == null ? false : (boolean) args[2];
            Integer maxTermsForChangeDetection = (Integer) args[3];

            return new TermsGroupSource(field, scriptConfig, missingBucket, maxTermsForChangeDetection);
        });

        SingleGroupSource.declareValuesSourceFields(parser, lenient);
        parser.declareInt(optionalConstructorArg(), TransformField.MAX_TERMS_FOR_CHANGE_DETECTION);
        return parser;
    }

    @Nullable
    private final Integer maxTermsForChangeDetection;

    public TermsGroupSource(final String field, final ScriptConfig scriptConfig, boolean missingBucket) {
        this(field, scriptConfig, missingBucket, null);
    }

    public TermsGroupSource(
        final String field,
        final ScriptConfig scriptConfig,
        boolean missingBucket,
        @Nullable Integer maxTermsForChangeDetection
    ) {
        super(field, scriptConfig, missingBucket);
        this.maxTermsForChangeDetection = maxTermsForChangeDetection;
    }

    public TermsGroupSource(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(MAX_TERMS_FOR_CHANGE_DETECTION)) {
            maxTermsForChangeDetection = in.readOptionalInt();
        } else {
            maxTermsForChangeDetection = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(MAX_TERMS_FOR_CHANGE_DETECTION)) {
            out.writeOptionalInt(maxTermsForChangeDetection);
        }
    }

    /**
     * Returns the configured maximum number of changed terms to collect during change detection
     * before disabling the terms-based optimization for this group.
     * <p>
     * Accepted values:
     * <ul>
     *   <li>{@code null} -- unlimited (default); never disable terms-based filtering</li>
     *   <li>{@code -1}   -- unlimited; equivalent to {@code null}</li>
     *   <li>{@code 0}    -- always disable terms-based filtering for this group</li>
     *   <li>positive integer -- the maximum number of changed terms before disabling filtering</li>
     * </ul>
     * <p>
     * This setting is part of the group-by configuration and cannot be updated after transform creation.
     * Overflow is re-evaluated at each checkpoint; if data characteristics change, the optimization
     * may re-engage on subsequent checkpoints.
     *
     * @return the configured value, or {@code null} if not set (unlimited)
     */
    @Nullable
    public Integer getMaxTermsForChangeDetection() {
        return maxTermsForChangeDetection;
    }

    @Override
    ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        validationException = super.validate(validationException);
        if (maxTermsForChangeDetection != null && maxTermsForChangeDetection < -1) {
            validationException = addValidationError(
                "max_terms_for_change_detection ["
                    + maxTermsForChangeDetection
                    + "] is out of range. Use -1 to disable the limit, 0 to disable change filtering,"
                    + " or a positive integer to set the limit",
                validationException
            );
        }
        return validationException;
    }

    @Override
    public Type getType() {
        return Type.TERMS;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (maxTermsForChangeDetection != null && excludeTransformMetadata(params) == false) {
            builder.field(TransformField.MAX_TERMS_FOR_CHANGE_DETECTION.getPreferredName(), maxTermsForChangeDetection);
        }
    }

    // Pivot builds the composite aggregation by serializing group sources to XContent and re-parsing
    // them with CompositeAggregationBuilder's strict parser. Transform-only fields like
    // max_terms_for_change_detection must be suppressed during that serialization path, otherwise the
    // parser rejects them as unknown. Pivot passes EXCLUDE_TRANSFORM_METADATA=true for this purpose.
    private static boolean excludeTransformMetadata(Params params) {
        return Boolean.parseBoolean(params.param(TransformField.EXCLUDE_TRANSFORM_METADATA, "false"));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        if (super.equals(other) == false) {
            return false;
        }
        final TermsGroupSource that = (TermsGroupSource) other;
        return Objects.equals(this.maxTermsForChangeDetection, that.maxTermsForChangeDetection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxTermsForChangeDetection);
    }

    public static TermsGroupSource fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
