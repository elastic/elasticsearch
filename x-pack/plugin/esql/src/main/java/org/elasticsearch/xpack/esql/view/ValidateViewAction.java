/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.esql.EsqlViewActionNames;

import java.io.IOException;
import java.util.Objects;

/**
 * Reports what is wrong with a view definition without storing it. Unlike {@link PutViewAction} this runs wherever it is received
 * rather than on the master, so that analyzing the query — which fans out field-caps and runs the full analyzer — does not load the
 * master node.
 */
public class ValidateViewAction extends ActionType<ActionResponse.Empty> {

    public static final ValidateViewAction INSTANCE = new ValidateViewAction();
    public static final String NAME = EsqlViewActionNames.ESQL_VALIDATE_VIEW_ACTION_NAME;

    private ValidateViewAction() {
        super(NAME);
    }

    public static class Request extends UntypedActionRequest implements IndicesRequest {
        private final View view;

        public Request(View view) {
            this.view = view;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            view = new View(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            view.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public View view() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return view.equals(((Request) o).view);
        }

        @Override
        public int hashCode() {
            return Objects.hash(view);
        }

        @Override
        public String[] indices() {
            return new String[] { view.getName() };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return PutViewAction.DEFAULT_INDICES_OPTIONS;
        }
    }
}
