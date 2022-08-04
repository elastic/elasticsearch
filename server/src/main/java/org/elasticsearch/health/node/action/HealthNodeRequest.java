/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * A base request for operation that need to be executed by the selected health node.
 */
public abstract class HealthNodeRequest<Request extends HealthNodeRequest<Request>> extends ActionRequest {

    public static final TimeValue DEFAULT_HEALTH_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    protected TimeValue healthNodeTimeout = DEFAULT_HEALTH_NODE_TIMEOUT;

    protected HealthNodeRequest() {}

    protected HealthNodeRequest(StreamInput in) throws IOException {
        super(in);
        healthNodeTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(healthNodeTimeout);
    }

    /**
     * A timeout value in case the health node has not been selected yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final Request healthNodeTimeout(TimeValue timeout) {
        this.healthNodeTimeout = timeout;
        return (Request) this;
    }

    public final TimeValue healthNodeTimeout() {
        return this.healthNodeTimeout;
    }
}
