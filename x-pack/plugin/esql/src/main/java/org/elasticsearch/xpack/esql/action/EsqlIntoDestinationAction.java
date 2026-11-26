/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;

/**
 * Asks the security filter whether the caller may write to the destination of an {@code INTO}, before anything about that destination
 * is reported. The action does nothing: it exists so that the filter authorizes and audits it, as the caller, like the write itself.
 * A caller who may not write gets the denial of the filter, which names the privileges that grant the action and is audited as
 * {@code access_denied}. The same holds for an existing and a missing destination, since the filter decides by name. Without
 * security the action simply succeeds.
 * <p>
 * The name is what ties the action to the privileges: it starts with {@code indices:data/write/index[}, which the privileges that
 * allow indexing a document ({@code create_doc}, {@code create}, {@code index}, {@code write}) grant, and which {@code delete} and
 * {@code read} do not. {@code EsqlIntoDestinationActionTests} fails if a change to the privileges breaks that.
 */
public class EsqlIntoDestinationAction extends TransportAction<EsqlIntoDestinationAction.Request, EsqlIntoDestinationAction.Response> {
    public static final String NAME = "indices:data/write/index[esql/into_destination]";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);

    @Inject
    public EsqlIntoDestinationAction(TransportService transportService, ActionFilters actionFilters) {
        super(NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        listener.onResponse(new Response());
    }

    /**
     * Local only, so it never crosses the wire. The destination is one concrete name: a pattern would be expanded by the security
     * filter, and an empty name list reads as {@code _all} to its resolver, so the caller of this request validates the name first.
     */
    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {
        private String[] indices;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        public Request(String destination) {
            this.indices = new String[] { destination };
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            // A destination that does not exist is authorized by its name too, and an unauthorized one is a denial, not a narrowing
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }

        @Override
        public String toString() {
            return "EsqlIntoDestinationAction.Request{indices:" + Arrays.toString(indices) + "}";
        }
    }

    public static class Response extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }
}
