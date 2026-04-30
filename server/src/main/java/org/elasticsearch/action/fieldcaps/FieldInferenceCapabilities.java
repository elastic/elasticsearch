/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Container for the inference-related portion of a field's capabilities.
 *
 * <p>Holding the inference fields together in a single class keeps the {@link FieldCapabilities} and {@link IndexFieldCapabilities}
 * surfaces narrow: future additions (e.g. {@code task_type}, {@code chunking_settings}) extend this class only and don't ripple into
 * other constructors or wire formats.
 *
 * <p>Per-shard responses (in {@link IndexFieldCapabilities}) only set {@link #inferenceId} and {@link #searchInferenceId}.
 * The cross-index merge in {@link FieldCapabilities.Builder} additionally populates {@link #inferenceConflictsIndices} when indices
 * disagree on the inference id.
 */
public record FieldInferenceCapabilities(
    @Nullable String inferenceId,
    @Nullable String searchInferenceId,
    @Nullable String[] inferenceConflictsIndices
) implements Writeable, ToXContentObject {

    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField SEARCH_INFERENCE_ID_FIELD = new ParseField("search_inference_id");
    public static final ParseField INFERENCE_CONFLICTS_INDICES_FIELD = new ParseField("inference_conflicts_indices");

    public static final ConstructingObjectParser<FieldInferenceCapabilities, Void> PARSER = new ConstructingObjectParser<>(
        "field_capabilities_inference",
        true,
        args -> new FieldInferenceCapabilities((String) args[0], (String) args[1], asStringArray(args[2]))
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SEARCH_INFERENCE_ID_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INFERENCE_CONFLICTS_INDICES_FIELD);
    }

    /** Constructor for the per-shard case where conflict info is never present. */
    public FieldInferenceCapabilities(@Nullable String inferenceId, @Nullable String searchInferenceId) {
        this(inferenceId, searchInferenceId, null);
    }

    public static FieldInferenceCapabilities readFrom(StreamInput in) throws IOException {
        return new FieldInferenceCapabilities(in.readOptionalString(), in.readOptionalString(), in.readOptionalStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(inferenceId);
        out.writeOptionalString(searchInferenceId);
        out.writeOptionalStringArray(inferenceConflictsIndices);
    }

    /**
     * @return true if no inference information is carried (e.g. the field is not backed by an inference endpoint at all).
     */
    public boolean isEmpty() {
        return inferenceId == null && searchInferenceId == null && inferenceConflictsIndices == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inferenceId != null) {
            builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        }
        if (searchInferenceId != null) {
            builder.field(SEARCH_INFERENCE_ID_FIELD.getPreferredName(), searchInferenceId);
        }
        if (inferenceConflictsIndices != null) {
            builder.array(INFERENCE_CONFLICTS_INDICES_FIELD.getPreferredName(), inferenceConflictsIndices);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldInferenceCapabilities that = (FieldInferenceCapabilities) o;
        return Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(searchInferenceId, that.searchInferenceId)
            && Arrays.equals(inferenceConflictsIndices, that.inferenceConflictsIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceId, searchInferenceId, Arrays.hashCode(inferenceConflictsIndices));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @SuppressWarnings("unchecked")
    private static String[] asStringArray(Object value) {
        if (value == null) {
            return null;
        }
        return ((List<String>) value).toArray(new String[0]);
    }
}
