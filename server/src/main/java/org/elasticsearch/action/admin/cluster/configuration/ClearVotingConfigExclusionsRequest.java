/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * A request to clear the voting config exclusions from the cluster state, optionally waiting for these nodes to be removed from the
 * cluster first.
 */
public class ClearVotingConfigExclusionsRequest extends MasterNodeRequest<ClearVotingConfigExclusionsRequest> {
    private boolean waitForRemoval = true;
    private TimeValue timeout = TimeValue.timeValueSeconds(30);

    /**
     * Construct a request to remove all the voting config exclusions from the cluster state.
     */
    public ClearVotingConfigExclusionsRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    public ClearVotingConfigExclusionsRequest(StreamInput in) throws IOException {
        super(in);
        waitForRemoval = in.readBoolean();
        timeout = in.readTimeValue();
    }

    /**
     * @return whether to wait for the currently excluded nodes to be removed from the cluster before removing their exclusions.
     * True by default.
     */
    public boolean getWaitForRemoval() {
        return waitForRemoval;
    }

    /**
     * @param waitForRemoval whether to wait for the currently excluded nodes to be removed from the cluster before removing their
     *                       exclusions. True by default.
     */
    public void setWaitForRemoval(boolean waitForRemoval) {
        this.waitForRemoval = waitForRemoval;
    }

    /**
     * @param timeout how long to wait for the excluded nodes to be removed if {@link ClearVotingConfigExclusionsRequest#waitForRemoval} is
     *                true. Defaults to 30 seconds.
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * @return how long to wait for the excluded nodes to be removed if {@link ClearVotingConfigExclusionsRequest#waitForRemoval} is
     * true. Defaults to 30 seconds.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(waitForRemoval);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "ClearVotingConfigExclusionsRequest{" + ", waitForRemoval=" + waitForRemoval + ", timeout=" + timeout + '}';
    }
}
