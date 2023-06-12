/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.action;

import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction.Response;
import co.elastic.elasticsearch.stateless.autoscaling.model.StatelessAutoscalingMetrics;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Rest action binding for Stateless Autoscaling Metrics API
 */
public class GetStatelessAutoscalingMetricsAction extends ActionType<Response> {

    public static final GetStatelessAutoscalingMetricsAction INSTANCE = new GetStatelessAutoscalingMetricsAction();
    public static final String NAME = "cluster:admin/stateless/autoscaling/get_stateless_autoscaling_metrics";

    public GetStatelessAutoscalingMetricsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        public Request() {}

        public Request(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final StatelessAutoscalingMetrics autoscalingMetrics;

        public Response(final StatelessAutoscalingMetrics autoscalingMetrics) {
            this.autoscalingMetrics = autoscalingMetrics;
        }

        public Response(final StreamInput input) throws IOException {
            super(input);
            this.autoscalingMetrics = new StatelessAutoscalingMetrics(input);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.value(autoscalingMetrics);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            autoscalingMetrics.writeTo(out);
        }
    }
}
