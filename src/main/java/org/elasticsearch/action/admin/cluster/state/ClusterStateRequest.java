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
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class ClusterStateRequest extends MasterNodeOperationRequest<ClusterStateRequest> {

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metaData = true;
    private boolean blocks = true;
    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] indexTemplates = Strings.EMPTY_ARRAY;
    private boolean local = false;

    public ClusterStateRequest() {
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
        indices = Strings.EMPTY_ARRAY;
        indexTemplates = Strings.EMPTY_ARRAY;
        return this;
    }
    
    public ClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metaData = false;
        blocks = false;
        indices = Strings.EMPTY_ARRAY;
        indexTemplates = Strings.EMPTY_ARRAY;
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

    public String[] indices() {
        return indices;
    }

    public ClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public String[] indexTemplates() {
        return this.indexTemplates;
    }

    public ClusterStateRequest indexTemplates(String... indexTemplates) {
        this.indexTemplates = indexTemplates;
        return this;
    }

    public ClusterStateRequest local(boolean local) {
        this.local = local;
        return this;
    }

    public boolean local() {
        return this.local;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        routingTable = in.readBoolean();
        nodes = in.readBoolean();
        metaData = in.readBoolean();
        blocks = in.readBoolean();
        indices = in.readStringArray();
        indexTemplates = in.readStringArray();
        local = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metaData);
        out.writeBoolean(blocks);
        out.writeStringArray(indices);
        out.writeStringArray(indexTemplates);
        out.writeBoolean(local);
    }
}
