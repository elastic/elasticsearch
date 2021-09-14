/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class WarningInferenceResults implements InferenceResults {

    public static final String NAME = "warning";
    public static final ParseField WARNING = new ParseField("warning");

    private final String warning;

    public WarningInferenceResults(String warning, Object... args) {
        this(LoggerMessageFormat.format(warning, args));
    }

    public WarningInferenceResults(String warning) {
        this.warning = warning;
    }

    public WarningInferenceResults(StreamInput in) throws IOException {
        this.warning = in.readString();
    }

    public String getWarning() {
        return warning;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(warning);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        WarningInferenceResults that = (WarningInferenceResults) object;
        return Objects.equals(warning, that.warning);
    }

    @Override
    public int hashCode() {
        return Objects.hash(warning);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> asMap = new LinkedHashMap<>();
        asMap.put(NAME, warning);
        return asMap;
    }

    @Override
    public Object predictedValue() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, warning);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
