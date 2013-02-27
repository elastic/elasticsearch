/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.node.restart;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.readTimeValue;

/**
 * A request to restart one ore more nodes (or the whole cluster).
 */
public class NodesRestartRequest extends NodesOperationRequest<NodesRestartRequest> {

    TimeValue delay = TimeValue.timeValueSeconds(1);

    protected NodesRestartRequest() {
    }

    /**
     * Restarts down nodes based on the nodes ids specified. If none are passed, <b>all</b>
     * nodes will be shutdown.
     */
    public NodesRestartRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * The delay for the restart to occur. Defaults to <tt>1s</tt>.
     */
    public NodesRestartRequest delay(TimeValue delay) {
        this.delay = delay;
        return this;
    }

    /**
     * The delay for the restart to occur. Defaults to <tt>1s</tt>.
     */
    public NodesRestartRequest delay(String delay) {
        return delay(TimeValue.parseTimeValue(delay, null));
    }

    public TimeValue delay() {
        return this.delay;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        delay = readTimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        delay.writeTo(out);
    }
}
