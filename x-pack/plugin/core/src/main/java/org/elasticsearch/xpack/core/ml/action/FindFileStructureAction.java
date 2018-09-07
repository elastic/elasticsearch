/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindFileStructureAction extends Action<FindFileStructureAction.Response> {

    public static final FindFileStructureAction INSTANCE = new FindFileStructureAction();
    public static final String NAME = "cluster:monitor/xpack/ml/findfilestructure";

    private FindFileStructureAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client, FindFileStructureAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject, Writeable {

        private FileStructure fileStructure;

        public Response(FileStructure fileStructure) {
            this.fileStructure = fileStructure;
        }

        Response() {
        }

        public FileStructure getFileStructure() {
            return fileStructure;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fileStructure = new FileStructure(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fileStructure.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            fileStructure.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileStructure);
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            FindFileStructureAction.Response that = (FindFileStructureAction.Response) other;
            return Objects.equals(fileStructure, that.fileStructure);
        }
    }

    public static class Request extends ActionRequest {

        public static final ParseField LINES_TO_SAMPLE = new ParseField("lines_to_sample");

        private Integer linesToSample;
        private BytesReference sample;

        public Request() {
        }

        public Integer getLinesToSample() {
            return linesToSample;
        }

        public void setLinesToSample(Integer linesToSample) {
            this.linesToSample = linesToSample;
        }

        public BytesReference getSample() {
            return sample;
        }

        public void setSample(BytesReference sample) {
            this.sample = sample;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (linesToSample != null && linesToSample <= 0) {
                validationException =
                    addValidationError(LINES_TO_SAMPLE.getPreferredName() + " must be positive if specified", validationException);
            }
            if (sample == null || sample.length() == 0) {
                validationException = addValidationError("sample must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            linesToSample = in.readOptionalVInt();
            sample = in.readBytesReference();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalVInt(linesToSample);
            out.writeBytesReference(sample);
        }

        @Override
        public int hashCode() {
            return Objects.hash(linesToSample, sample);
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
                Objects.equals(this.sample, that.sample);
        }
    }
}
