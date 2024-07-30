/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * An update that sets the tokenization truncate option to NONE
 * and updates the span and max sequence length settings.
 */
public class TokenizationConfigUpdate implements InferenceConfigUpdate {

    public static final String NAME = "tokenization_update";

    private final Tokenization.SpanSettings spanSettings;

    public TokenizationConfigUpdate(@Nullable Integer maxSequenceLength, @Nullable Integer span) {
        this(span == null ? new Tokenization.SpanSettings(maxSequenceLength) : new Tokenization.SpanSettings(maxSequenceLength, span));
    }

    private TokenizationConfigUpdate(Tokenization.SpanSettings spanSettings) {
        this.spanSettings = spanSettings;
    }

    public TokenizationConfigUpdate(StreamInput in) throws IOException {
        this.spanSettings = new Tokenization.SpanSettings(in);
    }

    public Tokenization.SpanSettings getSpanSettings() {
        return spanSettings;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        spanSettings.writeTo(out);
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return true;
    }

    @Override
    public String getResultsField() {
        return null;
    }

    @Override
    public Builder<? extends Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        throw new UnsupportedOperationException("Tokenization update is not supported as a builder");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenizationConfigUpdate that = (TokenizationConfigUpdate) o;
        return Objects.equals(spanSettings, that.spanSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spanSettings);
    }
}
