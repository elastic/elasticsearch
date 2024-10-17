/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AdaptiveAllocationsSettings implements ToXContentObject, Writeable {

    public static final AdaptiveAllocationsSettings RESET_PLACEHOLDER = new AdaptiveAllocationsSettings(false, -1, -1);

    public static final ParseField ENABLED = new ParseField("enabled");
    public static final ParseField MIN_NUMBER_OF_ALLOCATIONS = new ParseField("min_number_of_allocations");
    public static final ParseField MAX_NUMBER_OF_ALLOCATIONS = new ParseField("max_number_of_allocations");

    public static final ObjectParser<AdaptiveAllocationsSettings.Builder, Void> PARSER = new ObjectParser<>(
        "autoscaling_settings",
        AdaptiveAllocationsSettings.Builder::new
    );

    static {
        PARSER.declareBoolean(Builder::setEnabled, ENABLED);
        PARSER.declareIntOrNull(Builder::setMinNumberOfAllocations, -1, MIN_NUMBER_OF_ALLOCATIONS);
        PARSER.declareIntOrNull(Builder::setMaxNumberOfAllocations, -1, MAX_NUMBER_OF_ALLOCATIONS);
    }

    public static AdaptiveAllocationsSettings parseRequest(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static class Builder {
        private Boolean enabled;
        private Integer minNumberOfAllocations;
        private Integer maxNumberOfAllocations;

        public Builder() {}

        public Builder(AdaptiveAllocationsSettings settings) {
            enabled = settings.enabled;
            minNumberOfAllocations = settings.minNumberOfAllocations;
            maxNumberOfAllocations = settings.maxNumberOfAllocations;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public void setMinNumberOfAllocations(Integer minNumberOfAllocations) {
            this.minNumberOfAllocations = minNumberOfAllocations;
        }

        public void setMaxNumberOfAllocations(Integer maxNumberOfAllocations) {
            this.maxNumberOfAllocations = maxNumberOfAllocations;
        }

        public AdaptiveAllocationsSettings build() {
            return new AdaptiveAllocationsSettings(enabled, minNumberOfAllocations, maxNumberOfAllocations);
        }
    }

    private final Boolean enabled;
    private final Integer minNumberOfAllocations;
    private final Integer maxNumberOfAllocations;

    public AdaptiveAllocationsSettings(Boolean enabled, Integer minNumberOfAllocations, Integer maxNumberOfAllocations) {
        this.enabled = enabled;
        this.minNumberOfAllocations = minNumberOfAllocations;
        this.maxNumberOfAllocations = maxNumberOfAllocations;
    }

    public AdaptiveAllocationsSettings(StreamInput in) throws IOException {
        enabled = in.readOptionalBoolean();
        minNumberOfAllocations = in.readOptionalInt();
        maxNumberOfAllocations = in.readOptionalInt();
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public Integer getMinNumberOfAllocations() {
        return minNumberOfAllocations;
    }

    public Integer getMaxNumberOfAllocations() {
        return maxNumberOfAllocations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED.getPreferredName(), enabled != null ? enabled : false);
        if (minNumberOfAllocations != null) {
            builder.field(MIN_NUMBER_OF_ALLOCATIONS.getPreferredName(), minNumberOfAllocations);
        }
        if (maxNumberOfAllocations != null) {
            builder.field(MAX_NUMBER_OF_ALLOCATIONS.getPreferredName(), maxNumberOfAllocations);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(enabled);
        out.writeOptionalInt(minNumberOfAllocations);
        out.writeOptionalInt(maxNumberOfAllocations);
    }

    public AdaptiveAllocationsSettings merge(AdaptiveAllocationsSettings updates) {
        AdaptiveAllocationsSettings.Builder builder = new Builder(this);
        if (updates.getEnabled() != null) {
            builder.setEnabled(updates.enabled);
        }
        if (updates.minNumberOfAllocations != null) {
            if (updates.minNumberOfAllocations == -1) {
                builder.setMinNumberOfAllocations(null);
            } else {
                builder.setMinNumberOfAllocations(updates.minNumberOfAllocations);
            }
        }
        if (updates.maxNumberOfAllocations != null) {
            if (updates.maxNumberOfAllocations == -1) {
                builder.setMaxNumberOfAllocations(null);
            } else {
                builder.setMaxNumberOfAllocations(updates.maxNumberOfAllocations);
            }
        }
        return builder.build();
    }

    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = new ActionRequestValidationException();
        boolean hasMinNumberOfAllocations = (minNumberOfAllocations != null && minNumberOfAllocations != -1);
        if (hasMinNumberOfAllocations && minNumberOfAllocations < 0) {
            validationException.addValidationError("[" + MIN_NUMBER_OF_ALLOCATIONS + "] must be a non-negative integer or null");
        }
        boolean hasMaxNumberOfAllocations = (maxNumberOfAllocations != null && maxNumberOfAllocations != -1);
        if (hasMaxNumberOfAllocations && maxNumberOfAllocations < 1) {
            validationException.addValidationError("[" + MAX_NUMBER_OF_ALLOCATIONS + "] must be a positive integer or null");
        }
        if (hasMinNumberOfAllocations && hasMaxNumberOfAllocations && minNumberOfAllocations > maxNumberOfAllocations) {
            validationException.addValidationError(
                "[" + MIN_NUMBER_OF_ALLOCATIONS + "] must not be larger than [" + MAX_NUMBER_OF_ALLOCATIONS + "]"
            );
        }
        return validationException.validationErrors().isEmpty() ? null : validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdaptiveAllocationsSettings that = (AdaptiveAllocationsSettings) o;
        return Objects.equals(enabled, that.enabled)
            && Objects.equals(minNumberOfAllocations, that.minNumberOfAllocations)
            && Objects.equals(maxNumberOfAllocations, that.maxNumberOfAllocations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, minNumberOfAllocations, maxNumberOfAllocations);
    }
}
