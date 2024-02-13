/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindStructureAction extends ActionType<FindStructureAction.Response> {

    public static final FindStructureAction INSTANCE = new FindStructureAction();
    public static final String NAME = "cluster:monitor/text_structure/findstructure";

    private FindStructureAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject, Writeable {

        private final TextStructure textStructure;

        public Response(TextStructure textStructure) {
            this.textStructure = textStructure;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            textStructure = new TextStructure(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            textStructure.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            textStructure.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(textStructure);
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            FindStructureAction.Response that = (FindStructureAction.Response) other;
            return Objects.equals(textStructure, that.textStructure);
        }
    }

    public static class Request extends AbstractFindStructureRequest {

        private BytesReference sample;

        public Request() {}

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
            return Objects.hash(super.hashCode(), sample);
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
            return super.equals(other) && Objects.equals(this.sample, that.sample);
        }
    }
}
