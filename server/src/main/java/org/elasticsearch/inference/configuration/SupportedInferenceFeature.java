/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Base class for an {@link InferenceFeature} whose entire contract is whether a service supports it, serialized as
 * <pre>{@code {"supported": true}}</pre>
 * <p>
 * Features that need to report more than a yes/no answer should implement {@link InferenceFeature} directly rather than
 * bolting extra fields onto this shape, so that the meaning of {@code supported} stays the same everywhere it appears.
 */
public abstract class SupportedInferenceFeature implements InferenceFeature {
    public static final ParseField SUPPORTED_FIELD = new ParseField("supported");

    /**
     * Builds a lenient parser for the {@code supported} field, which subclasses can extend with their own declarations.
     * Leniency here is deliberate: unknown fields within a feature object are ignored so that an older node can still
     * read a feature written by a newer one.
     */
    public static <B extends Builder<? extends SupportedInferenceFeature>> ObjectParser<B, Void> buildCommonParser(
        String featureName,
        Supplier<B> builderSupplier
    ) {
        var objectParser = new ObjectParser<B, Void>(featureName, true, builderSupplier);
        objectParser.declareBoolean(Builder::supported, SUPPORTED_FIELD);
        return objectParser;
    }

    public abstract static class Builder<T extends SupportedInferenceFeature> {
        private Boolean supported;

        public void supported(boolean supported) {
            this.supported = supported;
        }

        protected abstract T build(boolean supported);

        public final T build() {
            if (supported == null) {
                throw new IllegalArgumentException(Strings.format("Missing required field [%s]", SUPPORTED_FIELD.getPreferredName()));
            }

            return build(supported);
        }
    }

    private final boolean supported;

    protected SupportedInferenceFeature(boolean supported) {
        this.supported = supported;
    }

    protected SupportedInferenceFeature(StreamInput in) throws IOException {
        this.supported = in.readBoolean();
    }

    public boolean isSupported() {
        return supported;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(supported);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUPPORTED_FIELD.getPreferredName(), supported);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (SupportedInferenceFeature) o;
        return supported == that.supported;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), supported);
    }

    @Override
    public String toString() {
        return Strings.format("%s[supported=%b]", getWriteableName(), supported);
    }
}
