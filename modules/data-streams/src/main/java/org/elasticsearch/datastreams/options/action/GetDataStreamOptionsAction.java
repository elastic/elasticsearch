/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.options.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This action retrieves the data stream options from every data stream. Currently, data stream options only support
 * failure store.
 */
public class GetDataStreamOptionsAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("indices:admin/data_stream/options/get");

    private GetDataStreamOptionsAction() {/* no instances */}

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder().matchOpen(true).matchClosed(true).allowEmptyExpressions(true).resolveAliases(false)
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder().allowAliasToMultipleIndices(false).allowClosedIndices(true).allowSelectors(false)
            )
            .build();
        private boolean includeDefaults = false;

        public Request(TimeValue masterNodeTimeout, String[] names) {
            super(masterNodeTimeout);
            this.names = names;
        }

        public Request(TimeValue masterNodeTimeout, String[] names, boolean includeDefaults) {
            super(masterNodeTimeout);
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

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readOptionalStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            this.includeDefaults = in.readBoolean();
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

        public record DataStreamEntry(String dataStreamName, DataStreamOptions dataStreamOptions) implements Writeable, ToXContentObject {

            public static final ParseField NAME_FIELD = new ParseField("name");
            public static final ParseField OPTIONS_FIELD = new ParseField("options");

            /**
             * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
             * we no longer need to support calling this action remotely.
             */
            @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(dataStreamName);
                out.writeOptionalWriteable(dataStreamOptions);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(NAME_FIELD.getPreferredName(), dataStreamName);
                if (dataStreamOptions != null && dataStreamOptions.isEmpty() == false) {
                    builder.field(OPTIONS_FIELD.getPreferredName(), dataStreamOptions);
                }
                builder.endObject();
                return builder;
            }
        }

        private final List<DataStreamEntry> dataStreams;

        public Response(List<DataStreamEntry> dataStreams) {
            this.dataStreams = dataStreams;
        }

        public List<DataStreamEntry> getDataStreams() {
            return dataStreams;
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreams);
        }

        @Override
        public Iterator<ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                builder.startArray(DATA_STREAMS_FIELD.getPreferredName());
                return builder;
            }),
                Iterators.map(dataStreams.iterator(), entry -> (builder, params) -> entry.toXContent(builder, outerParams)),
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
            return dataStreams.equals(response.dataStreams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreams);
        }
    }
}
