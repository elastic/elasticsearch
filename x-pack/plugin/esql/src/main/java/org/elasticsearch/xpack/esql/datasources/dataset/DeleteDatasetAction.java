/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteDatasetAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDatasetAction INSTANCE = new DeleteDatasetAction();
    public static final String NAME = EsqlDatasetActionNames.ESQL_DELETE_DATASET_ACTION_NAME;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).build())
        .build();

    private DeleteDatasetAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {
        private String[] names;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String[] names) {
            super(masterNodeTimeout, ackTimeout);
            this.names = Objects.requireNonNull(names, "names cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (CollectionUtils.isEmpty(names)) {
                return addValidationError("dataset names cannot be empty", null);
            }
            return null;
        }

        public String[] names() {
            return names;
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return DEFAULT_INDICES_OPTIONS;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }
    }
}
