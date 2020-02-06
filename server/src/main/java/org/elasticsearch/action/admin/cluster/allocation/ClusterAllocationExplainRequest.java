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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to explain the allocation of a shard in the cluster
 */
public class ClusterAllocationExplainRequest extends MasterNodeRequest<ClusterAllocationExplainRequest> {

    private static final ObjectParser<ClusterAllocationExplainRequest, Void> PARSER = new ObjectParser<>("cluster/allocation/explain");
    static {
        PARSER.declareString(ClusterAllocationExplainRequest::setIndex, new ParseField("index"));
        PARSER.declareInt(ClusterAllocationExplainRequest::setShard, new ParseField("shard"));
        PARSER.declareBoolean(ClusterAllocationExplainRequest::setPrimary, new ParseField("primary"));
        PARSER.declareString(ClusterAllocationExplainRequest::setCurrentNode, new ParseField("current_node"));
    }

    @Nullable
    private String index;
    @Nullable
    private Integer shard;
    @Nullable
    private Boolean primary;
    @Nullable
    private String currentNode;
    private boolean includeYesDecisions = false;
    private boolean includeDiskInfo = false;

    /**
     * Create a new allocation explain request to explain any unassigned shard in the cluster.
     */
    public ClusterAllocationExplainRequest() {
        this.index = null;
        this.shard = null;
        this.primary = null;
        this.currentNode = null;
    }

    public ClusterAllocationExplainRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readOptionalString();
        this.shard = in.readOptionalVInt();
        this.primary = in.readOptionalBoolean();
        this.currentNode = in.readOptionalString();
        this.includeYesDecisions = in.readBoolean();
        this.includeDiskInfo = in.readBoolean();
    }

    /**
     * Create a new allocation explain request. If {@code primary} is false, the first unassigned replica
     * will be picked for explanation. If no replicas are unassigned, the first assigned replica will
     * be explained.
     *
     * Package private for testing.
     */
    ClusterAllocationExplainRequest(String index, int shard, boolean primary, @Nullable String currentNode) {
        this.index = index;
        this.shard = shard;
        this.primary = primary;
        this.currentNode = currentNode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(index);
        out.writeOptionalVInt(shard);
        out.writeOptionalBoolean(primary);
        out.writeOptionalString(currentNode);
        out.writeBoolean(includeYesDecisions);
        out.writeBoolean(includeDiskInfo);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (this.useAnyUnassignedShard() == false) {
            if (this.index == null) {
                validationException = addValidationError("index must be specified", validationException);
            }
            if (this.shard == null) {
                validationException = addValidationError("shard must be specified", validationException);
            }
            if (this.primary == null) {
                validationException = addValidationError("primary must be specified", validationException);
            }
        }
        return validationException;
    }

    /**
     * Returns {@code true} iff the first unassigned shard is to be used
     */
    public boolean useAnyUnassignedShard() {
        return this.index == null && this.shard == null && this.primary == null && this.currentNode == null;
    }

    /**
     * Sets the index name of the shard to explain.
     */
    public ClusterAllocationExplainRequest setIndex(String index) {
        this.index = index;
        return this;
    }

    /**
     * Returns the index name of the shard to explain, or {@code null} to use any unassigned shard (see {@link #useAnyUnassignedShard()}).
     */
    @Nullable
    public String getIndex() {
        return this.index;
    }

    /**
     * Sets the shard id of the shard to explain.
     */
    public ClusterAllocationExplainRequest setShard(Integer shard) {
        this.shard = shard;
        return this;
    }

    /**
     * Returns the shard id of the shard to explain, or {@code null} to use any unassigned shard (see {@link #useAnyUnassignedShard()}).
     */
    @Nullable
    public Integer getShard() {
        return this.shard;
    }

    /**
     * Sets whether to explain the allocation of the primary shard or a replica shard copy
     * for the shard id (see {@link #getShard()}).
     */
    public ClusterAllocationExplainRequest setPrimary(Boolean primary) {
        this.primary = primary;
        return this;
    }

    /**
     * Returns {@code true} if explaining the primary shard for the shard id (see {@link #getShard()}),
     * {@code false} if explaining a replica shard copy for the shard id, or {@code null} to use any
     * unassigned shard (see {@link #useAnyUnassignedShard()}).
     */
    @Nullable
    public Boolean isPrimary() {
        return this.primary;
    }

    /**
     * Requests the explain API to explain an already assigned replica shard currently allocated to
     * the given node.
     */
    public ClusterAllocationExplainRequest setCurrentNode(String currentNodeId) {
        this.currentNode = currentNodeId;
        return this;
    }

    /**
     * Returns the node holding the replica shard to be explained.  Returns {@code null} if any replica shard
     * can be explained.
     */
    @Nullable
    public String getCurrentNode() {
        return currentNode;
    }

    /**
     * Set to {@code true} to include yes decisions for a particular node.
     */
    public void includeYesDecisions(boolean includeYesDecisions) {
        this.includeYesDecisions = includeYesDecisions;
    }

    /**
     * Returns {@code true} if yes decisions should be included.  Otherwise only "no" and "throttle"
     * decisions are returned.
     */
    public boolean includeYesDecisions() {
        return this.includeYesDecisions;
    }

    /**
     * Set to {@code true} to include information about the gathered disk information of nodes in the cluster.
     */
    public void includeDiskInfo(boolean includeDiskInfo) {
        this.includeDiskInfo = includeDiskInfo;
    }

    /**
     * Returns {@code true} if information about disk usage and shard sizes should also be returned.
     */
    public boolean includeDiskInfo() {
        return this.includeDiskInfo;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ClusterAllocationExplainRequest[");
        if (this.useAnyUnassignedShard()) {
            sb.append("useAnyUnassignedShard=true");
        } else {
            sb.append("index=").append(index);
            sb.append(",shard=").append(shard);
            sb.append(",primary?=").append(primary);
            if (currentNode != null) {
                sb.append(",currentNode=").append(currentNode);
            }
        }
        sb.append(",includeYesDecisions?=").append(includeYesDecisions);
        return sb.toString();
    }

    public static ClusterAllocationExplainRequest parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new ClusterAllocationExplainRequest(), null);
    }
}
