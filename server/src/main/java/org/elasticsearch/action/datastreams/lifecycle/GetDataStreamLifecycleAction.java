/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * This action retrieves the data stream lifecycle from every data stream that has a data stream lifecycle configured.
 */
public class GetDataStreamLifecycleAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("indices:admin/data_stream/lifecycle/get");

    private GetDataStreamLifecycleAction() {/* no instances */}

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);
        private boolean includeDefaults = false;

        public Request(String[] names) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.names = names;
        }

        public Request(String[] names, boolean includeDefaults) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.names = names;
            this.includeDefaults = includeDefaults;
        }

        public String[] getNames() {
            return names;
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names)
                && indicesOptions.equals(request.indicesOptions)
                && includeDefaults == request.includeDefaults;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indicesOptions, includeDefaults);
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

        public boolean includeDefaults() {
            return includeDefaults;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
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
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        public static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");

        public record DataStreamLifecycle(
            String dataStreamName,
            @Nullable org.elasticsearch.cluster.metadata.DataStreamLifecycle lifecycle,
            boolean isSystemDataStream
        ) implements Writeable, ToXContentObject {

            public static final ParseField NAME_FIELD = new ParseField("name");
            public static final ParseField LIFECYCLE_FIELD = new ParseField("lifecycle");

            DataStreamLifecycle(StreamInput in) throws IOException {
                this(
                    in.readString(),
                    in.readOptionalWriteable(org.elasticsearch.cluster.metadata.DataStreamLifecycle::new),
                    in.getTransportVersion().onOrAfter(TransportVersions.NO_GLOBAL_RETENTION_FOR_SYSTEM_DATA_STREAMS) && in.readBoolean()
                );
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(dataStreamName);
                out.writeOptionalWriteable(lifecycle);
                if (out.getTransportVersion().onOrAfter(TransportVersions.NO_GLOBAL_RETENTION_FOR_SYSTEM_DATA_STREAMS)) {
                    out.writeBoolean(isSystemDataStream);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return toXContent(builder, params, null, null);
            }

            /**
             * Converts the response to XContent and passes the RolloverConditions and the global retention, when provided,
             * to the data stream lifecycle.
             */
            public XContentBuilder toXContent(
                XContentBuilder builder,
                Params params,
                @Nullable RolloverConfiguration rolloverConfiguration,
                @Nullable DataStreamGlobalRetention globalRetention
            ) throws IOException {
                builder.startObject();
                builder.field(NAME_FIELD.getPreferredName(), dataStreamName);
                if (lifecycle != null) {
                    builder.field(LIFECYCLE_FIELD.getPreferredName());
                    lifecycle.toXContent(
                        builder,
                        org.elasticsearch.cluster.metadata.DataStreamLifecycle.maybeAddEffectiveRetentionParams(params),
                        rolloverConfiguration,
                        isSystemDataStream ? null : globalRetention
                    );
                }
                builder.endObject();
                return builder;
            }
        }

        private final List<DataStreamLifecycle> dataStreamLifecycles;
        @Nullable
        private final RolloverConfiguration rolloverConfiguration;
        @Nullable
        private final DataStreamGlobalRetention globalRetention;

        public Response(List<DataStreamLifecycle> dataStreamLifecycles) {
            this(dataStreamLifecycles, null, null);
        }

        public Response(
            List<DataStreamLifecycle> dataStreamLifecycles,
            @Nullable RolloverConfiguration rolloverConfiguration,
            @Nullable DataStreamGlobalRetention globalRetention
        ) {
            this.dataStreamLifecycles = dataStreamLifecycles;
            this.rolloverConfiguration = rolloverConfiguration;
            this.globalRetention = globalRetention;
        }

        public Response(StreamInput in) throws IOException {
            this(
                in.readCollectionAsList(DataStreamLifecycle::new),
                in.readOptionalWriteable(RolloverConfiguration::new),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)
                    ? in.readOptionalWriteable(DataStreamGlobalRetention::read)
                    : null
            );
        }

        public List<DataStreamLifecycle> getDataStreamLifecycles() {
            return dataStreamLifecycles;
        }

        @Nullable
        public RolloverConfiguration getRolloverConfiguration() {
            return rolloverConfiguration;
        }

        public DataStreamGlobalRetention getGlobalRetention() {
            return globalRetention;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreamLifecycles);
            out.writeOptionalWriteable(rolloverConfiguration);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeOptionalWriteable(globalRetention);
            }
        }

        @Override
        public Iterator<ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                builder.startArray(DATA_STREAMS_FIELD.getPreferredName());
                return builder;
            }),
                Iterators.map(
                    dataStreamLifecycles.iterator(),
                    dataStreamLifecycle -> (builder, params) -> dataStreamLifecycle.toXContent(
                        builder,
                        outerParams,
                        rolloverConfiguration,
                        globalRetention
                    )
                ),
                Iterators.single((builder, params) -> {
                    builder.endArray();
                    builder.endObject();
                    return builder;
                })
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return dataStreamLifecycles.equals(response.dataStreamLifecycles)
                && Objects.equals(rolloverConfiguration, response.rolloverConfiguration)
                && Objects.equals(globalRetention, response.globalRetention);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreamLifecycles, rolloverConfiguration, globalRetention);
        }
    }
}
