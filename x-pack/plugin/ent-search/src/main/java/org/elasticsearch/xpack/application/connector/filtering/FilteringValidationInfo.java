/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Encapsulates validation information for filtering rules, including any errors encountered
 * during validation and the overall state of validation.
 *
 * This class holds a list of validation errors, each represented by a {@link FilteringValidation} object,
 * and the validation state, indicated by a {@link FilteringValidationState}.
 */
public class FilteringValidationInfo implements Writeable, ToXContentObject {

    private final List<FilteringValidation> validationErrors;
    private final FilteringValidationState validationState;

    /**
     * @param validationErrors The list of {@link FilteringValidation} errors for the filtering rules.
     * @param validationState  The {@link FilteringValidationState} of the filtering rules.
     */
    public FilteringValidationInfo(List<FilteringValidation> validationErrors, FilteringValidationState validationState) {
        this.validationErrors = validationErrors;
        this.validationState = validationState;
    }

    public FilteringValidationInfo(StreamInput in) throws IOException {
        this.validationErrors = in.readCollectionAsList(FilteringValidation::new);
        this.validationState = in.readEnum(FilteringValidationState.class);
    }

    private static final ParseField ERRORS_FIELD = new ParseField("errors");
    private static final ParseField STATE_FIELD = new ParseField("state");

    public FilteringValidationState getValidationState() {
        return validationState;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FilteringValidationInfo, Void> PARSER = new ConstructingObjectParser<>(
        "filtering_validation_info",
        true,
        args -> new Builder().setValidationErrors((List<FilteringValidation>) args[0])
            .setValidationState((FilteringValidationState) args[1])
            .build()
    );

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> FilteringValidation.fromXContent(p), ERRORS_FIELD);
        PARSER.declareField(
            constructorArg(),
            (p, c) -> FilteringValidationState.filteringValidationState(p.text()),
            STATE_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ERRORS_FIELD.getPreferredName(), validationErrors);
            builder.field(STATE_FIELD.getPreferredName(), validationState);
        }
        builder.endObject();
        return builder;
    }

    public static FilteringValidationInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(validationErrors);
        out.writeEnum(validationState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilteringValidationInfo that = (FilteringValidationInfo) o;
        return Objects.equals(validationErrors, that.validationErrors) && validationState == that.validationState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(validationErrors, validationState);
    }

    public static FilteringValidationInfo getInitialDraftValidationInfo() {
        return new FilteringValidationInfo.Builder().setValidationErrors(Collections.emptyList())
            .setValidationState(FilteringValidationState.EDITED)
            .build();
    }

    public static class Builder {

        private List<FilteringValidation> validationErrors;
        private FilteringValidationState validationState;

        public Builder setValidationErrors(List<FilteringValidation> validationErrors) {
            this.validationErrors = validationErrors;
            return this;
        }

        public Builder setValidationState(FilteringValidationState validationState) {
            this.validationState = validationState;
            return this;
        }

        public FilteringValidationInfo build() {
            return new FilteringValidationInfo(validationErrors, validationState);
        }
    }
}
