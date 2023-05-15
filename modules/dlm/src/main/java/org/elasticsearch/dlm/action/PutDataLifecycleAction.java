/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.DataLifecycle.DATA_RETENTION_FIELD;

/**
 * Sets the data lifecycle that was provided in the request to the requested data streams.
 */
public class PutDataLifecycleAction extends ActionType<AcknowledgedResponse> {

    public static final PutDataLifecycleAction INSTANCE = new PutDataLifecycleAction();
    public static final String NAME = "indices:admin/dlm/put";

    private PutDataLifecycleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static final class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable, ToXContentObject {

        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "put_data_stream_lifecycle_request",
            args -> new Request(null, ((TimeValue) args[0]))
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DATA_RETENTION_FIELD.getPreferredName()),
                DATA_RETENTION_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
        }

        public static Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);
        private final DataLifecycle lifecycle;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            lifecycle = new DataLifecycle(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            out.writeWriteable(lifecycle);
        }

        public Request(String[] names, @Nullable TimeValue dataRetention) {
            this.names = names;
            this.lifecycle = new DataLifecycle(dataRetention);
        }

        public String[] getNames() {
            return names;
        }

        public DataLifecycle getLifecycle() {
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
        public ActionRequestValidationException validate() {
            return null;
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
