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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class ClusterStateRequest extends MasterNodeReadRequest<ClusterStateRequest> implements IndicesRequest.Replaceable {

    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metaData = true;
    private boolean blocks = true;
    private boolean customs = true;
    private Long waitForMetaDataVersion;
    private TimeValue waitForTimeout = DEFAULT_WAIT_FOR_NODE_TIMEOUT;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

    public ClusterStateRequest() {
    }

    public ClusterStateRequest(StreamInput in) throws IOException {
        super(in);
        routingTable = in.readBoolean();
        nodes = in.readBoolean();
        metaData = in.readBoolean();
        blocks = in.readBoolean();
        customs = in.readBoolean();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        waitForTimeout = in.readTimeValue();
        waitForMetaDataVersion = in.readOptionalLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metaData);
        out.writeBoolean(blocks);
        out.writeBoolean(customs);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(waitForTimeout);
        out.writeOptionalLong(waitForMetaDataVersion);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ClusterStateRequest all() {
        routingTable = true;
        nodes = true;
        metaData = true;
        blocks = true;
        customs = true;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public ClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metaData = false;
        blocks = false;
        customs = false;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public boolean routingTable() {
        return routingTable;
    }

    public ClusterStateRequest routingTable(boolean routingTable) {
        this.routingTable = routingTable;
        return this;
    }

    public boolean nodes() {
        return nodes;
    }

    public ClusterStateRequest nodes(boolean nodes) {
        this.nodes = nodes;
        return this;
    }

    public boolean metaData() {
        return metaData;
    }

    public ClusterStateRequest metaData(boolean metaData) {
        this.metaData = metaData;
        return this;
    }

    public boolean blocks() {
        return blocks;
    }

    public ClusterStateRequest blocks(boolean blocks) {
        this.blocks = blocks;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public final ClusterStateRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public ClusterStateRequest customs(boolean customs) {
        this.customs = customs;
        return this;
    }

    public boolean customs() {
        return customs;
    }

    public TimeValue waitForTimeout() {
        return waitForTimeout;
    }

    public ClusterStateRequest waitForTimeout(TimeValue waitForTimeout) {
        this.waitForTimeout = waitForTimeout;
        return this;
    }

    public Long waitForMetaDataVersion() {
        return waitForMetaDataVersion;
    }

    public ClusterStateRequest waitForMetaDataVersion(long waitForMetaDataVersion) {
        if (waitForMetaDataVersion < 1) {
            throw new IllegalArgumentException("provided waitForMetaDataVersion should be >= 1, but instead is [" +
                waitForMetaDataVersion + "]");
        }
        this.waitForMetaDataVersion = waitForMetaDataVersion;
        return this;
    }
}
