/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
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
 * Action for explaining the data stream lifecycle status for one or more indices.
 */
public class ExplainDataStreamLifecycleAction {

    public static final ActionType<ExplainDataStreamLifecycleAction.Response> INSTANCE = new ActionType<>(
        "indices:admin/data_stream/lifecycle/explain"
    );

    private ExplainDataStreamLifecycleAction() {/* no instances */}

    /**
     * Request explaining the data stream lifecycle for one or more indices.
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

        @Override
        public boolean includeDataStreams() {
            return true;
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
     * Class representing the response for the 'explain' of the data stream lifecycle action for one or more indices.
     */
    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        public static final ParseField INDICES_FIELD = new ParseField("indices");
        private final List<ExplainIndexDataStreamLifecycle> indices;
        @Nullable
        private final RolloverConfiguration rolloverConfiguration;
        @Nullable
        private final DataStreamGlobalRetention globalRetention;

        public Response(
            List<ExplainIndexDataStreamLifecycle> indices,
            @Nullable RolloverConfiguration rolloverConfiguration,
            @Nullable DataStreamGlobalRetention globalRetention
        ) {
            this.indices = indices;
            this.rolloverConfiguration = rolloverConfiguration;
            this.globalRetention = globalRetention;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readCollectionAsList(ExplainIndexDataStreamLifecycle::new);
            this.rolloverConfiguration = in.readOptionalWriteable(RolloverConfiguration::new);
            this.globalRetention = in.getTransportVersion().onOrAfter(TransportVersions.USE_DATA_STREAM_GLOBAL_RETENTION)
                ? in.readOptionalWriteable(DataStreamGlobalRetention::read)
                : null;
        }

        public List<ExplainIndexDataStreamLifecycle> getIndices() {
            return indices;
        }

        public RolloverConfiguration getRolloverConfiguration() {
            return rolloverConfiguration;
        }

        public DataStreamGlobalRetention getGlobalRetention() {
            return globalRetention;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(indices);
            out.writeOptionalWriteable(rolloverConfiguration);
            if (out.getTransportVersion().onOrAfter(TransportVersions.USE_DATA_STREAM_GLOBAL_RETENTION)) {
                out.writeOptionalWriteable(globalRetention);
            }
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
            return Objects.equals(indices, response.indices)
                && Objects.equals(rolloverConfiguration, response.rolloverConfiguration)
                && Objects.equals(globalRetention, response.globalRetention);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, rolloverConfiguration, globalRetention);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                builder.startObject(INDICES_FIELD.getPreferredName());
                return builder;
            }), Iterators.map(indices.iterator(), explainIndexDataLifecycle -> (builder, params) -> {
                builder.field(explainIndexDataLifecycle.getIndex());
                ToXContent.Params withEffectiveRetentionParams = new ToXContent.DelegatingMapParams(
                    DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS,
                    params
                );
                explainIndexDataLifecycle.toXContent(builder, withEffectiveRetentionParams, rolloverConfiguration, globalRetention);
                return builder;
            }), Iterators.single((builder, params) -> {
                builder.endObject();
                builder.endObject();
                return builder;
            }));
        }
    }
}
