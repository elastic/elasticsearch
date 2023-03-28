/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.application.search.SearchApplicationQueryParams;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class QuerySearchApplicationAction extends ActionType<SearchResponse> {

    public static final QuerySearchApplicationAction INSTANCE = new QuerySearchApplicationAction();
    public static final String NAME = "cluster:admin/xpack/application/search_application/search";

    public QuerySearchApplicationAction() {
        super(NAME, SearchResponse::new);
    }

    public static class Request extends ActionRequest {
        private final String name;

        private final SearchApplicationQueryParams queryParams;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.queryParams = new SearchApplicationQueryParams(in);
        }

        public Request(String name, SearchApplicationQueryParams queryParams) {
            this.name = name;
            this.queryParams = queryParams;
        }

        public String name() {
            return name;
        }

        public SearchApplicationQueryParams queryParams() {
            return queryParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isEmpty(name)) {
                validationException = addValidationError("Search Application name is missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            queryParams.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QuerySearchApplicationAction.Request request = (QuerySearchApplicationAction.Request) o;
            return Objects.equals(name, request.name) && Objects.equals(queryParams, request.queryParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, queryParams);
        }
    }
}
