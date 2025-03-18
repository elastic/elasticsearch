/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class RenderSearchApplicationQueryAction {

    public static final String NAME = "cluster:admin/xpack/application/search_application/render_query";
    public static final ActionType<RenderSearchApplicationQueryAction.Response> INSTANCE = new ActionType<>(NAME);

    private RenderSearchApplicationQueryAction() {/* no instances */}

    public static class Response extends ActionResponse implements ToXContentObject, NamedWriteable {

        private final SearchSourceBuilder searchSourceBuilder;

        public Response(StreamInput in) throws IOException {
            this.searchSourceBuilder = new SearchSourceBuilder(in);
        }

        public Response(String name, SearchSourceBuilder searchSourceBuilder) {
            this.searchSourceBuilder = searchSourceBuilder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            searchSourceBuilder.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return searchSourceBuilder.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return searchSourceBuilder.equals(response.searchSourceBuilder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchSourceBuilder);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
