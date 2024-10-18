/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class FlushTrainedModelCacheAction extends ActionType<AcknowledgedResponse> {

    public static final FlushTrainedModelCacheAction INSTANCE = new FlushTrainedModelCacheAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/clear_model_cache";

    private FlushTrainedModelCacheAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<FlushTrainedModelCacheAction.Request> {
        public Request() {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        }

        Request(TimeValue timeout) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(ackTimeout());
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) return true;
            if (other == null || getClass() != other.getClass()) return false;
            Request that = (Request) other;
            return Objects.equals(that.ackTimeout(), ackTimeout());
        }
    }
}
