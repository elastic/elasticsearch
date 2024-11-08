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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Sets the data stream options that was provided in the request to the requested data streams.
 */
public class PutDataStreamOptionsAction {

    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("indices:admin/data_stream/options/put");

    private PutDataStreamOptionsAction() {/* no instances */}

    public static final class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {

        public interface Factory {
            Request create(@Nullable DataStreamFailureStore dataStreamFailureStore);
        }

        public static final ConstructingObjectParser<Request, Factory> PARSER = new ConstructingObjectParser<>(
            "put_data_stream_options_request",
            false,
            (args, factory) -> factory.create((DataStreamFailureStore) args[0])
        );

        static {
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> DataStreamFailureStore.PARSER.parse(p, null),
                null,
                new ParseField("failure_store")
            );
        }

        public static Request parseRequest(XContentParser parser, Factory factory) {
            return PARSER.apply(parser, factory);
        }

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder().matchOpen(true).matchClosed(true).allowEmptyExpressions(true).resolveAliases(false)
            )
            .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowAliasToMultipleIndices(false).allowClosedIndices(true))
            .build();
        private final DataStreamOptions options;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            options = DataStreamOptions.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            out.writeWriteable(options);
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String[] names, DataStreamOptions options) {
            super(masterNodeTimeout, ackTimeout);
            this.names = names;
            this.options = options;
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String[] names, @Nullable DataStreamFailureStore failureStore) {
            super(masterNodeTimeout, ackTimeout);
            this.names = names;
            this.options = new DataStreamOptions(failureStore);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (options.failureStore() == null) {
                validationException = addValidationError("At least one option needs to be provided", validationException);
            }
            return validationException;
        }

        public String[] getNames() {
            return names;
        }

        public DataStreamOptions getOptions() {
            return options;
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
                && options.equals(request.options);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indicesOptions, options);
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
