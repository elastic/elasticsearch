/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GetSearchEngineAction extends ActionType<GetSearchEngineAction.Response> {

    public static final GetSearchEngineAction INSTANCE = new GetSearchEngineAction();
    public static final String NAME = "indices:admin/search_engine/get";

    private GetSearchEngineAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<GetSearchEngineAction.Request> implements IndicesRequest.Replaceable {

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);

        public Request(String[] names) {
            this.names = names;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readOptionalStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(names);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetSearchEngineAction.Request request = (GetSearchEngineAction.Request) o;
            return Arrays.equals(names, request.names) && indicesOptions.equals(request.indicesOptions);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indicesOptions);
            result = 31 * result + Arrays.hashCode(names);
            return result;
        }

        @Override
        public String[] indices() {
            return names;
        }

        public String[] getNames() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return false;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField NAME_FIELD = new ParseField("name");
        public static final ParseField INDICES_FIELD = new ParseField("indices");

        public static final ParseField RELEVANCE_SETTINGS_ID_FIELD = new ParseField("relevance_settings_id");

        private final List<SearchEngine> searchEngines;

        public Response(List<SearchEngine> searchEngines) {
            this.searchEngines = searchEngines;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readList(SearchEngine::new));
        }

        public List<SearchEngine> getSearchEngines() {
            return searchEngines;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(searchEngines);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetSearchEngineAction.Response response = (GetSearchEngineAction.Response) o;
            return searchEngines.equals(response.searchEngines);
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchEngines);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();

            // TODO: can't we reuse searchEngine.toXContent?
            for (SearchEngine searchEngine : searchEngines) {
                builder.field(searchEngine.getName());
                builder.startObject();

                builder.field(NAME_FIELD.getPreferredName(), searchEngine.getName());

                if (searchEngine.getRelevanceSettingsId() != null) {
                    builder.field(RELEVANCE_SETTINGS_ID_FIELD.getPreferredName(), searchEngine.getRelevanceSettingsId());
                }

                builder.startArray(INDICES_FIELD.getPreferredName());
                for (Index index : searchEngine.getIndices()) {
                    builder.value(index.getName());
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }
}
