/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

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
