/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class WaitForSnapshotStep extends AsyncWaitStep {

    static final String NAME = "wait-for-snapshot";

    private static final String POLICY_NOT_EXECUTED_MESSAGE = "waiting for policy '%s' to be executed";
    private static final String POLICY_NOT_FOUND_MESSAGE = "policy '%s' not found, waiting for it to be created and executed";

    private final String policy;

    WaitForSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String policy) {
        super(key, nextStepKey, client);
        this.policy = policy;
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
        Client client = getClient();
        ResponseHandler handler = new ResponseHandler(listener);

        ExplainLifecycleRequest explainLifecycleReq = new ExplainLifecycleRequest();
        String index = indexMetaData.getIndex().getName();
        explainLifecycleReq.indices(index);
        client.execute(ExplainLifecycleAction.INSTANCE, explainLifecycleReq,
            ActionListener.wrap(r -> handler.handleResponse(-r.getIndexResponses().get(index).getPhaseTime()), listener::onFailure));

        GetSnapshotLifecycleAction.Request getSnapshotReq = new GetSnapshotLifecycleAction.Request(policy);
        client.execute(GetSnapshotLifecycleAction.INSTANCE, getSnapshotReq,
            ActionListener.wrap(r -> handler.handleResponse(r.getPolicies().get(0).getLastSuccess().getTimestamp()),
                e -> handleSnapshotRequestFailure(listener, e)));
    }

    private void handleSnapshotRequestFailure(Listener listener, Exception e) {
        if (e instanceof ResourceNotFoundException) {
            listener.onResponse(false, new Info(POLICY_NOT_FOUND_MESSAGE));
        } else {
            listener.onFailure(e);
        }
    }

    private final class ResponseHandler {
        private final AtomicLong timeDifference = new AtomicLong();
        private final AtomicBoolean requestsFinished = new AtomicBoolean();
        private final Listener listener;


        public ResponseHandler(Listener listener) {
            this.listener = listener;
        }

        private void handleResponse(long time) {
            timeDifference.addAndGet(time);
            if (requestsFinished.getAndSet(true)) {
                if (timeDifference.get() > 0) {
                    listener.onResponse(true, null);
                } else {
                    listener.onResponse(false, new Info(POLICY_NOT_EXECUTED_MESSAGE));
                }
            }
        }
    }

    private final class Info implements ToXContentObject {

        private static final String MESSAGE_FIELD = "message";
        private final String message;

        private Info(String message) {
            this.message = message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE_FIELD, String.format(message, policy));
            builder.endObject();
            return builder;
        }
    }
}
