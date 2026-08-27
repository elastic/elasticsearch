/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

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
import org.elasticsearch.xpack.core.esql.EsqlViewActionNames;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteViewAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteViewAction INSTANCE = new DeleteViewAction();
    public static final String NAME = EsqlViewActionNames.ESQL_DELETE_VIEW_ACTION_NAME;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveViews(true).build())
        .build();

    private DeleteViewAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {
        private String[] views;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String[] views) {
            super(masterNodeTimeout, ackTimeout);
            this.views = Objects.requireNonNull(views, "views cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            views = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(views);
        }

        public String[] views() {
            return views;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (CollectionUtils.isEmpty(views)) {
                validationException = addValidationError("views cannot be null or missing", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(views, request.views);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(views);
        }

        @Override
        public String[] indices() {
            return views;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.views = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return DEFAULT_INDICES_OPTIONS;
        }
    }
}
