/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.dlm.ExplainIndexDataLifecycle;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Action for explaining the DLM lifecycle status for one or more indices.
 */
public class ExplainDataLifecycleAction extends ActionType<ExplainDataLifecycleAction.Response> {
    public static final ExplainDataLifecycleAction INSTANCE = new ExplainDataLifecycleAction();
    public static final String NAME = "indices:admin/data_stream/lifecycle/explain";

    public ExplainDataLifecycleAction() {
        super(NAME, Response::new);
    }

    /**
     * Request explaining the DLM lifecycle for one or more indices.
     */
    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {
        private String[] names;
        private boolean includeDefaults;
        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        public Request(String[] names) {
            this(names, false);
        }

        public Request(String[] names, boolean includeDefaults) {
            this.names = names;
            this.includeDefaults = includeDefaults;
        }

        public boolean includeDefaults() {
            return includeDefaults;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readOptionalStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            this.includeDefaults = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            out.writeBoolean(includeDefaults);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request request = (Request) o;
            return includeDefaults == request.includeDefaults
                && Arrays.equals(names, request.names)
                && Objects.equals(indicesOptions, request.indicesOptions);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(includeDefaults, indicesOptions);
            result = 31 * result + Arrays.hashCode(names);
            return result;
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        public Request includeDefaults(boolean includeDefaults) {
            this.includeDefaults = includeDefaults;
            return this;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }
    }

    /**
     * Class representing the response for the explain DLM lifecycle action for one or more indices.
     */
    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        public static final ParseField INDICES_FIELD = new ParseField("indices");
        private List<ExplainIndexDataLifecycle> indices;
        @Nullable
        private final RolloverConfiguration rolloverConfiguration;

        public Response(List<ExplainIndexDataLifecycle> indices, @Nullable RolloverConfiguration rolloverConfiguration) {
            this.indices = indices;
            this.rolloverConfiguration = rolloverConfiguration;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readList(ExplainIndexDataLifecycle::new);
            this.rolloverConfiguration = in.readOptionalWriteable(RolloverConfiguration::new);
        }

        public List<ExplainIndexDataLifecycle> getIndices() {
            return indices;
        }

        public RolloverConfiguration getRolloverConfiguration() {
            return rolloverConfiguration;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(indices);
            out.writeOptionalWriteable(rolloverConfiguration);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response response = (Response) o;
            return Objects.equals(indices, response.indices) && Objects.equals(rolloverConfiguration, response.rolloverConfiguration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, rolloverConfiguration);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            final Iterator<? extends ToXContent> indicesIterator = indices.stream()
                .map(explainIndexDataLifecycle -> (ToXContent) (builder, params) -> {
                    builder.field(explainIndexDataLifecycle.getIndex());
                    explainIndexDataLifecycle.toXContent(builder, params, rolloverConfiguration);
                    return builder;
                })
                .iterator();

            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                builder.startObject(INDICES_FIELD.getPreferredName());
                return builder;
            }), indicesIterator, Iterators.single((ToXContent) (builder, params) -> {
                builder.endObject();
                builder.endObject();
                return builder;
            }));
        }
    }
}
