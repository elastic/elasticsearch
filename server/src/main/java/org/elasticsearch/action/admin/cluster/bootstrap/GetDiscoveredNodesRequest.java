/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class GetDiscoveredNodesRequest extends ActionRequest {

    private int waitForNodes = 1;
    private TimeValue timeout = TimeValue.ZERO;

    public GetDiscoveredNodesRequest setWaitForNodes(int waitForNodes) {
        if (waitForNodes < 1) {
            throw new IllegalArgumentException("always finds at least one node, waiting for [" + waitForNodes + "] is not allowed");
        }
        this.waitForNodes = waitForNodes;
        return this;
    }

    public int getWaitForNodes() {
        return waitForNodes;
    }

    public GetDiscoveredNodesRequest timeout(TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("negative timeout of [" + timeout + "] is not allowed");
        }
        this.timeout = timeout;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        final ActionRequestValidationException actionRequestValidationException = new ActionRequestValidationException();

        assert waitForNodes > 0 : waitForNodes;
        if (timeout.compareTo(TimeValue.ZERO) > 0 && waitForNodes <= 1) {
            actionRequestValidationException.addValidationError(
                "always discovers at least one node, so a timeout of [" + timeout + "] is unnecessary");
        }

        return actionRequestValidationException.validationErrors().isEmpty() ? null : actionRequestValidationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        setWaitForNodes(in.readInt());
        timeout(in.readTimeValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(getWaitForNodes());
        out.writeTimeValue(timeout());
    }

    @Override
    public String toString() {
        return "GetDiscoveredNodesRequest{" +
            "waitForNodes=" + waitForNodes +
            ", timeout=" + timeout +
            '}';
    }
}
