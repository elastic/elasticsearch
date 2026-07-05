/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponse;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Internal action to store inference endpoints and return the results of the store operation. This should only be used internally and not
 * exposed via a REST API.
 * For the exposed REST API action see {@link PutInferenceModelAction}.
 */
public class StoreInferenceEndpointsAction extends ActionType<StoreInferenceEndpointsAction.Response> {

    public static final StoreInferenceEndpointsAction INSTANCE = new StoreInferenceEndpointsAction();
    public static final String NAME = "cluster:internal/xpack/inference/create_endpoints";

    public StoreInferenceEndpointsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private final List<Model> models;

        public Request(List<Model> models, TimeValue timeout) {
            super(timeout, DEFAULT_ACK_TIMEOUT);
            this.models = Objects.requireNonNull(models);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            models = in.readCollectionAsImmutableList(Model::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(models);
        }

        public List<Model> getModels() {
            return models;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(models, request.models);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(models);
        }
    }

    public static class Response extends ActionResponse {
        private final List<ModelStoreResponse> results;

        public Response(List<ModelStoreResponse> results) {
            this.results = results;
        }

        public Response(StreamInput in) throws IOException {
            results = in.readCollectionAsImmutableList(ModelStoreResponse::new);
        }

        public List<ModelStoreResponse> getResults() {
            return results;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(results);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(results, response.results);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results);
        }
    }
}
