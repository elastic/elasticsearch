/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

class WaitForYellowStep extends AsyncWaitStep {

    static final String NAME = "wait-for-yellow-step";

    WaitForYellowStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
        String indexName = indexMetaData.getIndex().getName();
        ClusterHealthRequest request = new ClusterHealthRequest(indexName);
        request.waitForYellowStatus();
        CheckedConsumer<ClusterHealthResponse, Exception> handler = clusterHealthResponse -> {
            boolean success = clusterHealthResponse.isTimedOut() == false;
            if (success) {
                listener.onResponse(success, null);
            } else {
                listener.onResponse(false, new Info());
            }
        };
        getClient().admin().cluster().health(request, ActionListener.wrap(handler, listener::onFailure));
    }

    static final class Info implements ToXContentObject {

        static final ParseField MESSAGE_FIELD = new ParseField("message");

        private final String message;

        Info() {
            this.message = "cluster health request timed out waiting for yellow status";
        }

        String getMessage() {
            return message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE_FIELD.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return Objects.equals(getMessage(), info.getMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getMessage());
        }
    }
}
