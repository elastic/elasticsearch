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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
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

    private static ObjectParser<ClusterAllocationExplainRequest, ParseFieldMatcherSupplier> PARSER = new ObjectParser(
            "cluster/allocation/explain");
    static {
        PARSER.declareString(ClusterAllocationExplainRequest::setIndex, new ParseField("index"));
        PARSER.declareInt(ClusterAllocationExplainRequest::setShard, new ParseField("shard"));
        PARSER.declareBoolean(ClusterAllocationExplainRequest::setPrimary, new ParseField("primary"));
    }

    private String index;
    private Integer shard;
    private Boolean primary;
    private boolean includeYesDecisions = false;

    /** Explain the first unassigned shard */
    public ClusterAllocationExplainRequest() {
        this.index = null;
        this.shard = null;
        this.primary = null;
    }

    /**
     * Create a new allocation explain request. If {@code primary} is false, the first unassigned replica
     * will be picked for explanation. If no replicas are unassigned, the first assigned replica will
     * be explained.
     */
    public ClusterAllocationExplainRequest(String index, int shard, boolean primary) {
        this.index = index;
        this.shard = shard;
        this.primary = primary;
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
        return this.index == null && this.shard == null && this.primary == null;
    }

    public ClusterAllocationExplainRequest setIndex(String index) {
        this.index = index;
        return this;
    }

    @Nullable
    public String getIndex() {
        return this.index;
    }

    public ClusterAllocationExplainRequest setShard(Integer shard) {
        this.shard = shard;
        return this;
    }

    @Nullable
    public Integer getShard() {
        return this.shard;
    }

    public ClusterAllocationExplainRequest setPrimary(Boolean primary) {
        this.primary = primary;
        return this;
    }

    @Nullable
    public Boolean isPrimary() {
        return this.primary;
    }

    public void includeYesDecisions(boolean includeYesDecisions) {
        this.includeYesDecisions = includeYesDecisions;
    }

    /** Returns true if all decisions should be included. Otherwise only "NO" and "THROTTLE" decisions are returned */
    public boolean includeYesDecisions() {
        return this.includeYesDecisions;
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
        }
        sb.append(",includeYesDecisions?=").append(includeYesDecisions);
        return sb.toString();
    }

    public static ClusterAllocationExplainRequest parse(XContentParser parser) throws IOException {
        ClusterAllocationExplainRequest req = PARSER.parse(parser, new ClusterAllocationExplainRequest(), () -> ParseFieldMatcher.STRICT);
        Exception e = req.validate();
        if (e != null) {
            throw new ElasticsearchParseException("'index', 'shard', and 'primary' must be specified in allocation explain request", e);
        }
        return req;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.index = in.readOptionalString();
        this.shard = in.readOptionalVInt();
        this.primary = in.readOptionalBoolean();
        this.includeYesDecisions = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(index);
        out.writeOptionalVInt(shard);
        out.writeOptionalBoolean(primary);
        out.writeBoolean(includeYesDecisions);
    }
}
