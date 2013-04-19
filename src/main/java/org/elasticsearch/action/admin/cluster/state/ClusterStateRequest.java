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

    public final static String NONE = "_na";

    private boolean filterRoutingTable = false;

    private boolean filterNodes = false;

    private boolean filterMetaData = false;

    private boolean filterBlocks = false;

    private String[] filteredIndices = Strings.EMPTY_ARRAY;

    private String[] filteredIndexTemplates = Strings.EMPTY_ARRAY;

    private boolean local = false;

    public ClusterStateRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ClusterStateRequest filterAll() {
        filterRoutingTable = true;
        filterNodes = true;
        filterMetaData = true;
        filterBlocks = true;
        filteredIndices = Strings.EMPTY_ARRAY;
        filteredIndexTemplates = Strings.EMPTY_ARRAY;
        return this;
    }

    public boolean filterRoutingTable() {
        return filterRoutingTable;
    }

    public ClusterStateRequest filterRoutingTable(boolean filterRoutingTable) {
        this.filterRoutingTable = filterRoutingTable;
        return this;
    }

    public boolean filterNodes() {
        return filterNodes;
    }

    public ClusterStateRequest filterNodes(boolean filterNodes) {
        this.filterNodes = filterNodes;
        return this;
    }

    public boolean filterMetaData() {
        return filterMetaData;
    }

    public ClusterStateRequest filterMetaData(boolean filterMetaData) {
        this.filterMetaData = filterMetaData;
        return this;
    }

    public boolean filterBlocks() {
        return filterBlocks;
    }

    public ClusterStateRequest filterBlocks(boolean filterBlocks) {
        this.filterBlocks = filterBlocks;
        return this;
    }

    public String[] filteredIndices() {
        return filteredIndices;
    }

    public ClusterStateRequest filterOutIndices() {
        this.filteredIndices = new String[]{NONE};
        return this;
    }

    public ClusterStateRequest filteredIndices(String... filteredIndices) {
        this.filteredIndices = filteredIndices;
        return this;
    }

    public String[] filteredIndexTemplates() {
        return this.filteredIndexTemplates;
    }

    public ClusterStateRequest filteredIndexTemplates(String... filteredIndexTemplates) {
        this.filteredIndexTemplates = filteredIndexTemplates;
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
        filterRoutingTable = in.readBoolean();
        filterNodes = in.readBoolean();
        filterMetaData = in.readBoolean();
        filterBlocks = in.readBoolean();
        filteredIndices = in.readStringArray();
        filteredIndexTemplates = in.readStringArray();
        local = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(filterRoutingTable);
        out.writeBoolean(filterNodes);
        out.writeBoolean(filterMetaData);
        out.writeBoolean(filterBlocks);
        out.writeStringArray(filteredIndices);
        out.writeStringArray(filteredIndexTemplates);
        out.writeBoolean(local);
    }
}
