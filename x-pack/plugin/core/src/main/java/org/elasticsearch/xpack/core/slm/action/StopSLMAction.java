/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

public class StopSLMAction extends ActionType<AcknowledgedResponse> {
    public static final StopSLMAction INSTANCE = new StopSLMAction();
    public static final String NAME = "cluster:admin/slm/stop";

    protected StopSLMAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
        }

        @Override
        public int hashCode() {
            return 85;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            return true;
        }
    }
}
