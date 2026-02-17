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
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.esql.EsqlViewActionNames;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutViewAction extends ActionType<AcknowledgedResponse> {

    public static final PutViewAction INSTANCE = new PutViewAction();
    public static final String NAME = EsqlViewActionNames.ESQL_PUT_VIEW_ACTION_NAME;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(IndicesOptions.WildcardOptions.builder().resolveViews(true).build())
        .build();

    private PutViewAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {
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
                // No further validation can be done, as the view is null
                return validationException;
            }
            final String name = view.name();
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
                validationException = addValidationError("invalid view name [" + name + "], must be lowercase", validationException);
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

        @Override
        public String[] indices() {
            return new String[] { view.getName() };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return DEFAULT_INDICES_OPTIONS;
        }
    }
}
