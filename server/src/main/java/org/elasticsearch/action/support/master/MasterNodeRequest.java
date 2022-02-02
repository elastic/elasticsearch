/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * A based request for master based operation.
 */
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ActionRequest {

    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    protected TimeValue masterNodeTimeout = DEFAULT_MASTER_NODE_TIMEOUT;

    protected MasterNodeRequest() {}

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(masterNodeTimeout);
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final Request masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    public final Request masterNodeTimeout(String timeout) {
        return masterNodeTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".masterNodeTimeout"));
    }

    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }
}
