/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutViewAction extends ActionType<AcknowledgedResponse> {

    public static final PutViewAction INSTANCE = new PutViewAction();
    public static final String NAME = "cluster:admin/xpack/esql/view/put";

    private PutViewAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private final View view;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, View view) {
            super(masterNodeTimeout, ackTimeout);
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
            ActionRequestValidationException validationException = null;
            if (view == null) {
                validationException = addValidationError("view is missing", validationException);
            }
            final String name = view.name();
            if (Strings.hasText(name) == false) {
                validationException = addValidationError("view name is missing or empty", validationException);
            }
            // The view name is used in a similar context to an index name and therefore has the same restrictions as an index name
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(
                    name,
                    (viewName, error) -> new IllegalArgumentException("invalid view name [" + name + "], " + error)
                );
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(e.getMessage(), validationException);
            }
            if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
                validationException = addValidationError("Invalid view name [" + name + "], must be lowercase", validationException);
            }
            final String query = view.query();
            if (Strings.hasText(query) == false) {
                validationException = addValidationError("view query is missing or empty", validationException);
            }
            return validationException;
        }

        public View view() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return view.equals(request.view);
        }

        @Override
        public int hashCode() {
            return Objects.hash(view);
        }
    }
}
