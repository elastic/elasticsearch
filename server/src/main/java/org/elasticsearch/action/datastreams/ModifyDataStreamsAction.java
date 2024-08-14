/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ModifyDataStreamsAction extends ActionType<AcknowledgedResponse> {

    public static final ModifyDataStreamsAction INSTANCE = new ModifyDataStreamsAction();
    public static final String NAME = "indices:admin/data_stream/modify";

    private ModifyDataStreamsAction() {
        super(NAME);
    }

    public static final class Request extends AcknowledgedRequest<Request> implements IndicesRequest, ToXContentObject {

        // The actual DataStreamAction don't support wildcards, so supporting it doesn't make sense.
        // Also supporting wildcards it would prohibit this api from removing broken references to backing indices. (in case of bugs).
        // For this case, when removing broken backing indices references that don't exist, we need to allow ignore_unavailable and
        // allow_no_indices. Otherwise, the data stream can't be repaired.
        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(
            true,
            true,
            false,
            false,
            false,
            false,
            true,
            false
        );

        private final List<DataStreamAction> actions;

        public Request(StreamInput in) throws IOException {
            super(in);
            actions = in.readCollectionAsList(DataStreamAction::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(actions);
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, List<DataStreamAction> actions) {
            super(masterNodeTimeout, ackTimeout);
            this.actions = Collections.unmodifiableList(actions);
        }

        public List<DataStreamAction> getActions() {
            return actions;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("actions");
            for (DataStreamAction action : actions) {
                action.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (actions.isEmpty()) {
                return addValidationError("must specify at least one data stream modification action", null);
            }
            return null;
        }

        public interface Factory {
            Request create(List<DataStreamAction> actions);
        }

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Request, Factory> PARSER = new ConstructingObjectParser<>(
            "data_stream_actions",
            false,
            (args, factory) -> factory.create((List<DataStreamAction>) args[0])
        );
        static {
            PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> DataStreamAction.PARSER.parse(p, null),
                new ParseField("actions")
            );
        }

        @Override
        public String[] indices() {
            // Return the indices instead of data streams, this api can be used to repair a broken data stream definition and
            // in that case, exceptions can occur while resolving data streams for doing authorization or looking up index blocks.
            return actions.stream().map(DataStreamAction::getIndex).toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.equals(actions.toArray(), other.actions.toArray());
        }

        @Override
        public int hashCode() {
            return Objects.hash(actions);
        }

    }
}
