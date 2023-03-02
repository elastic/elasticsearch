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
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.entsearch.engine.EngineListItem;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ListEnginesAction extends ActionType<ListEnginesAction.Response> {

    public static final ListEnginesAction INSTANCE = new ListEnginesAction();
    public static final String NAME = "cluster:admin/engine/list";

    public ListEnginesAction() {
        super(NAME, ListEnginesAction.Response::new);
    }

    // TODO Check if this should implement IndicesRequest.Replaceable
    public static class Request extends ActionRequest {

        private static final String DEFAULT_QUERY = "*";
        private final String query;
        private final PageParams pageParams;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.query = Objects.requireNonNullElse(in.readOptionalString(), DEFAULT_QUERY);
            this.pageParams = new PageParams(in);
        }

        public Request(@Nullable String query, PageParams pageParams) {
            this.query = Objects.requireNonNullElse(query, DEFAULT_QUERY);
            this.pageParams = pageParams;
        }

        public String query() {
            return query;
        }

        public PageParams pageParams() {
            return pageParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            // Pagination validation is done as part of PageParams constructor
            ActionRequestValidationException validationException = null;
            if (Strings.isEmpty(query())) {
                validationException = ValidateActions.addValidationError("Engine query is missing", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(query);
            pageParams.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(query, that.query) && Objects.equals(pageParams, that.pageParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, pageParams);
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        public static final ParseField ENGINES_RESULT_FIELD = new ParseField("results");

        final QueryPage<EngineListItem> queryPage;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.queryPage = new QueryPage<>(in, EngineListItem::new);
        }

        public Response(List<EngineListItem> engines, Long totalResults) {
            this.queryPage = new QueryPage<>(engines, totalResults, ENGINES_RESULT_FIELD);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            queryPage.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return queryPage.toXContent(builder, params);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return queryPage.equals(that.queryPage);
        }

        @Override
        public int hashCode() {
            return queryPage.hashCode();
        }
    }
}
