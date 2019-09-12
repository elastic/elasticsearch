/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class KillProcessAction extends ActionType<KillProcessAction.Response> {

    public static final KillProcessAction INSTANCE = new KillProcessAction();
    public static final String NAME = "cluster:internal/xpack/ml/job/kill/process";

    private KillProcessAction() {
        super(NAME, KillProcessAction.Response::new);
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client, KillProcessAction action) {
            super(client, action, new Request());
        }
    }

    public static class Request extends JobTaskRequest<Request> {

        public Request(String jobId) {
            super(jobId);
        }

        public Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private final boolean killed;

        public Response(StreamInput in) throws IOException {
            super(in);
            killed = in.readBoolean();
        }

        public Response(boolean killed) {
            super(null, null);
            this.killed = killed;
        }

        public boolean isKilled() {
            return killed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(killed);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return killed == response.killed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(killed);
        }
    }

}
