/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindStructureAction extends ActionType<TextStructureResponse> {

    public static final FindStructureAction INSTANCE = new FindStructureAction();
    public static final String NAME = "cluster:monitor/text_structure/findstructure";

    private FindStructureAction() {
        super(NAME, TextStructureResponse::new);
    }

    public static class Request extends AbstractFindStructureRequest {

        private BytesReference sample;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            sample = in.readBytesReference();
        }

        public BytesReference getSample() {
            return sample;
        }

        public void setSample(BytesReference sample) {
            this.sample = sample;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (sample == null || sample.length() == 0) {
                validationException = addValidationError("sample must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBytesReference(sample);
        }

        @Override
        public int hashCode() {
            return Objects.hash(linesToSample, lineMergeSizeLimit, timeout, charset, format, columnNames, hasHeaderRow, delimiter,
                grokPattern, timestampFormat, timestampField, sample);
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            Request that = (Request) other;
            return Objects.equals(this.linesToSample, that.linesToSample) &&
                Objects.equals(this.lineMergeSizeLimit, that.lineMergeSizeLimit) &&
                Objects.equals(this.timeout, that.timeout) &&
                Objects.equals(this.charset, that.charset) &&
                Objects.equals(this.format, that.format) &&
                Objects.equals(this.columnNames, that.columnNames) &&
                Objects.equals(this.hasHeaderRow, that.hasHeaderRow) &&
                Objects.equals(this.delimiter, that.delimiter) &&
                Objects.equals(this.grokPattern, that.grokPattern) &&
                Objects.equals(this.timestampFormat, that.timestampFormat) &&
                Objects.equals(this.timestampField, that.timestampField) &&
                Objects.equals(this.sample, that.sample);
        }
    }
}
