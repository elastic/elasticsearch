/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Base request for master based read operations that allows to read the cluster state from the local node if needed
 */
public abstract class MasterNodeReadRequest<Request extends MasterNodeReadRequest<Request>> extends MasterNodeRequest<Request> {

    protected boolean local = false;

    /**
     * @param masterNodeTimeout Specifies how long to wait when the master has not been discovered yet, or is disconnected, or is busy
     *                          processing other tasks:
     *                          <ul>
     *                          <li>
     *                              For requests which originate in the REST layer, use
     *                              {@link org.elasticsearch.rest.RestUtils#getMasterNodeTimeout} to determine the timeout.
     *                          </li>
     *                          <li>
     *                              For internally-generated requests, choose an appropriate timeout. Often this will be an infinite
     *                              timeout, {@link #INFINITE_MASTER_NODE_TIMEOUT}, since it is reasonable to wait for as long as necessary
     *                              for internal requests to complete.
     *                          </li>
     *                          </ul>
     */
    protected MasterNodeReadRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    protected MasterNodeReadRequest(StreamInput in) throws IOException {
        super(in);
        local = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(local);
    }

    @SuppressWarnings("unchecked")
    public final Request local(boolean local) {
        this.local = local;
        return (Request) this;
    }

    /**
     * Return local information, do not retrieve the state from master node (default: false).
     * @return <code>true</code> if local information is to be returned;
     * <code>false</code> if information is to be retrieved from master node (default).
     */
    public final boolean local() {
        return local;
    }
}
