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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public abstract class BaseNodesRequest<Request extends BaseNodesRequest<Request>> extends ActionRequest {

    /**
     * the list of nodesIds that will be used to resolve this request and {@link #concreteNodes}
     * will be populated. Note that if {@link #concreteNodes} is not null, it will be used and nodeIds
     * will be ignored.
     *
     * See {@link DiscoveryNodes#resolveNodes} for a full description of the options.
     *
     * TODO: we can get rid of this and resolve it to concrete nodes in the rest layer
     **/
    private String[] nodesIds;

    /**
     * once {@link #nodesIds} are resolved this will contain the concrete nodes that are part of this request. If set, {@link #nodesIds}
     * will be ignored and this will be used.
     * */
    private DiscoveryNode[] concreteNodes;

    private TimeValue timeout;

    protected BaseNodesRequest(StreamInput in) throws IOException {
        super(in);
        nodesIds = in.readStringArray();
        concreteNodes = in.readOptionalArray(DiscoveryNode::new, DiscoveryNode[]::new);
        timeout = in.readOptionalTimeValue();
    }

    protected BaseNodesRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    protected BaseNodesRequest(DiscoveryNode... concreteNodes) {
        this.nodesIds = null;
        this.concreteNodes = concreteNodes;
    }

    public final String[] nodesIds() {
        return nodesIds;
    }

    @SuppressWarnings("unchecked")
    public final Request nodesIds(String... nodesIds) {
        this.nodesIds = nodesIds;
        return (Request) this;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout");
        return (Request) this;
    }
    public DiscoveryNode[] concreteNodes() {
        return concreteNodes;
    }

    public void setConcreteNodes(DiscoveryNode[] concreteNodes) {
        this.concreteNodes = concreteNodes;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(nodesIds);
        out.writeOptionalArray(concreteNodes);
        out.writeOptionalTimeValue(timeout);
    }
}
