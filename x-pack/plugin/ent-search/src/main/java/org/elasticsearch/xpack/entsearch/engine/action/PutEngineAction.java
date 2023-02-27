/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.entsearch.engine.Engine;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutEngineAction extends ActionType<PutEngineAction.Response> {

    public static final PutEngineAction INSTANCE = new PutEngineAction();
    public static final String NAME = "indices:admin/engine/put";

    public PutEngineAction() {
        super(NAME, PutEngineAction.Response::new);
    }

    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        // indices options that require every specified index to exist, do not expand wildcards,
        // don't allow that no indices are resolved from wildcard expressions and resolve the
        // expressions only against indices
        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(
            false,
            false,
            false,
            false,
            true,
            false,
            true,
            false
        );

        private final Engine engine;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.engine = new Engine(in);
        }

        public Request(String engineId, BytesReference content, XContentType contentType) {
            this.engine = Engine.fromXContentBytes(engineId, content, contentType);
        }

        public Request(Engine engine) {
            this.engine = engine;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (engine.indices().length == 0) {
                validationException = addValidationError("indices are missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            engine.writeTo(out);
        }

        @Override
        public String[] indices() {
            return engine.indices();
        }

        @Override
        public IndicesRequest indices(String... indices) {
            Engine updatedEngine = new Engine(engine.name(), indices, engine.analyticsCollectionName());
            updatedEngine.setUpdatedAtMillis(System.currentTimeMillis());
            return new Request(updatedEngine);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        public Engine getEngine() {
            return engine;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(engine, that.engine);
        }

        @Override
        public int hashCode() {
            return Objects.hash(engine);
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        final DocWriteResponse.Result result;

        public Response(StreamInput in) throws IOException {
            super(in);
            result = DocWriteResponse.Result.readFrom(in);
        }

        public Response(DocWriteResponse.Result result) {
            this.result = result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.result.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", this.result.getLowercase());
            builder.endObject();
            return builder;
        }

        @Override
        public RestStatus status() {
            return switch (result) {
                case CREATED -> RestStatus.CREATED;
                case NOT_FOUND -> RestStatus.NOT_FOUND;
                default -> RestStatus.OK;
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(result, that.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }

    }

}
