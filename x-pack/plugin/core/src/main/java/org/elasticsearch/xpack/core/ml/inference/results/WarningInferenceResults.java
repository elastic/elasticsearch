/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class WarningInferenceResults implements InferenceResults {

    public static final String NAME = "warning";
    public static final ParseField WARNING = new ParseField("warning");

    private final String warning;

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
    public void writeResult(IngestDocument document, String parentResultField) {
        ExceptionsHelper.requireNonNull(document, "document");
        ExceptionsHelper.requireNonNull(parentResultField, "resultField");
        document.setFieldValue(parentResultField + "." + "warning", warning);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
