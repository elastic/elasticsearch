/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Objects;

public class MoveToStepAction extends ActionType<MoveToStepAction.Response> {
    public static final MoveToStepAction INSTANCE = new MoveToStepAction();
    public static final String NAME = "cluster:admin/ilm/_move/post";

    protected MoveToStepAction() {
        super(NAME, MoveToStepAction.Response::new);
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {
        static final ParseField CURRENT_KEY_FIELD = new ParseField("current_step");
        static final ParseField NEXT_KEY_FIELD = new ParseField("next_step");
        private static final ConstructingObjectParser<Request, String> PARSER =
            new ConstructingObjectParser<>("move_to_step_request", false,
                (a, index) -> {
                    StepKey currentStepKey = (StepKey) a[0];
                    StepKey nextStepKey = (StepKey) a[1];
                    return new Request(index, currentStepKey, nextStepKey);
                });
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> StepKey.parse(p), CURRENT_KEY_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> StepKey.parse(p), NEXT_KEY_FIELD);
        }

        private String index;
        private StepKey currentStepKey;
        private StepKey nextStepKey;

        public Request(String index, StepKey currentStepKey, StepKey nextStepKey) {
            this.index = index;
            this.currentStepKey = currentStepKey;
            this.nextStepKey = nextStepKey;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
            this.currentStepKey = new StepKey(in);
            this.nextStepKey = new StepKey(in);
        }

        public Request() {
        }

        public String getIndex() {
            return index;
        }

        public StepKey getCurrentStepKey() {
            return currentStepKey;
        }

        public StepKey getNextStepKey() {
            return nextStepKey;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public static Request parseRequest(String name, XContentParser parser) {
            return PARSER.apply(parser, name);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            currentStepKey.writeTo(out);
            nextStepKey.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, currentStepKey, nextStepKey);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(index, other.index) && Objects.equals(currentStepKey, other.currentStepKey)
                && Objects.equals(nextStepKey, other.nextStepKey);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(CURRENT_KEY_FIELD.getPreferredName(), currentStepKey)
                .field(NEXT_KEY_FIELD.getPreferredName(), nextStepKey)
                .endObject();
        }
    }
}
