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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.entsearch.engine.Engine;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

import java.io.IOException;
import java.util.Objects;

public class ListEnginesAction extends ActionType<ListEnginesAction.Response> {

    public static final ListEnginesAction INSTANCE = new ListEnginesAction();
    public static final String NAME = "indices:admin/engine/list";

    public ListEnginesAction() {
        super(NAME, Response::new);
    }

    // TODO Check if this should implement IndicesRequest.Replaceable
    public static class Request extends ActionRequest implements IndicesRequest {

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
            // TODO check params validation work in constructor
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(query);
            pageParams.writeTo(out);
        }

        @Override
        public String[] indices() {
            return new String[] { EngineIndexService.ENGINE_ALIAS_NAME };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
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

        final Engine[] engines;
        final PageParams pageParams;
        final Integer totalResults;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.pageParams = new PageParams(in);
            this.totalResults = in.readVInt();
            this.engines = in.readArray(Engine::new, Engine[]::new);
        }

        public Response(Engine[] engines, PageParams pageParams, Integer totalResults) {
            this.engines = engines;
            this.pageParams = pageParams;
            this.totalResults = totalResults;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pageParams.writeTo(out);
            out.writeVInt(totalResults);
            out.writeArray(engines);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startObject("_meta");
                {
                    builder.field("from", pageParams.getFrom());
                    builder.field("size", pageParams.getSize());
                    builder.field("total", totalResults);
                }
                builder.endObject();
            }

            builder.array("results", (Object[]) engines);
            builder.endObject();

            return builder;
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
            return Objects.equals(engines, that.engines)
                && Objects.equals(pageParams, that.pageParams)
                && Objects.equals(totalResults, that.totalResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(engines, pageParams, totalResults);
        }

    }

}
