/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisions;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class GetAutoscalingDecisionAction extends ActionType<GetAutoscalingDecisionAction.Response> {

    public static final GetAutoscalingDecisionAction INSTANCE = new GetAutoscalingDecisionAction();
    public static final String NAME = "cluster:admin/autoscaling/get_autoscaling_decision";

    private GetAutoscalingDecisionAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<GetAutoscalingDecisionAction.Request> {

        public Request() {

        }

        public Request(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SortedMap<String, AutoscalingDecisions> decisions;

        public Response(final SortedMap<String, AutoscalingDecisions> decisions) {
            this.decisions = Objects.requireNonNull(decisions);
        }

        public Response(final StreamInput in) throws IOException {
            super(in);
            decisions = new TreeMap<>(in.readMap(StreamInput::readString, AutoscalingDecisions::new));
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeMap(decisions, StreamOutput::writeString, (o, decision) -> decision.writeTo(o));
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.startArray("decisions");
                {
                    for (final Map.Entry<String, AutoscalingDecisions> decision : decisions.entrySet()) {
                        builder.startObject();
                        {
                            builder.field(decision.getKey(), decision.getValue());
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response response = (Response) o;
            return decisions.equals(response.decisions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(decisions);
        }

    }

}
