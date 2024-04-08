/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.Downsampling;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DATA_RETENTION_FIELD;
import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DOWNSAMPLING_FIELD;
import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.ENABLED_FIELD;

/**
 * Sets the data stream lifecycle that was provided in the request to the requested data streams.
 */
public class PutDataStreamLifecycleAction {

    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("indices:admin/data_stream/lifecycle/put");

    private PutDataStreamLifecycleAction() {/* no instances */}

    public static final class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable, ToXContentObject {

        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "put_data_stream_lifecycle_request",
            args -> new Request(null, ((TimeValue) args[0]), (Boolean) args[1], (Downsampling) args[2])
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DATA_RETENTION_FIELD.getPreferredName()),
                DATA_RETENTION_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                    return Downsampling.NULL;
                } else {
                    return new Downsampling(AbstractObjectParser.parseArray(p, c, Downsampling.Round::fromXContent));
                }
            }, DOWNSAMPLING_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL);
        }

        public static Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);
        private final DataStreamLifecycle lifecycle;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            lifecycle = new DataStreamLifecycle(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            out.writeWriteable(lifecycle);
        }

        public Request(String[] names, @Nullable TimeValue dataRetention) {
            this(names, dataRetention, null, null);
        }

        public Request(String[] names, DataStreamLifecycle lifecycle) {
            this.names = names;
            this.lifecycle = lifecycle;
        }

        public Request(String[] names, @Nullable TimeValue dataRetention, @Nullable Boolean enabled) {
            this(names, dataRetention, enabled, null);
        }

        public Request(String[] names, @Nullable TimeValue dataRetention, @Nullable Boolean enabled, @Nullable Downsampling downsampling) {
            this.names = names;
            this.lifecycle = DataStreamLifecycle.newBuilder()
                .dataRetention(dataRetention)
                .enabled(enabled == null || enabled)
                .downsampling(downsampling)
                .build();
        }

        public String[] getNames() {
            return names;
        }

        public DataStreamLifecycle getLifecycle() {
            return lifecycle;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("lifecycle", lifecycle);
            builder.endObject();
            return builder;
        }

        @Override
        public String[] indices() {
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
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names)
                && Objects.equals(indicesOptions, request.indicesOptions)
                && lifecycle.equals(request.lifecycle);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indicesOptions, lifecycle);
            result = 31 * result + Arrays.hashCode(names);
            return result;
        }

        @Override
        public IndicesRequest indices(String... names) {
            this.names = names;
            return this;
        }
    }
}
