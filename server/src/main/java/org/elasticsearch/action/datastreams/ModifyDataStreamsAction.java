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
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static final class Request extends AcknowledgedRequest<Request> implements IndicesRequest, ToXContentObject {

        // relevant only for authorizing the request, so require every specified
        // index to exist, expand wildcards only to open indices, prohibit
        // wildcard expressions that resolve to zero indices, and do not attempt
        // to resolve expressions as aliases
        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(
            false,
            false,
            true,
            false,
            true,
            false,
            true,
            false
        );

        private final List<DataStreamAction> actions;

        public Request(StreamInput in) throws IOException {
            super(in);
            actions = in.readList(DataStreamAction::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(actions);
        }

        public Request(List<DataStreamAction> actions) {
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

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_actions",
            args -> new Request(((List<DataStreamAction>) args[0]))
        );
        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), DataStreamAction.PARSER, new ParseField("actions"));
        }

        @Override
        public String[] indices() {
            return actions.stream().map(DataStreamAction::getDataStream).toArray(String[]::new);
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
