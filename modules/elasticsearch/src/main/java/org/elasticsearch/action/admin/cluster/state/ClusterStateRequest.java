/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
 * @author kimchy (shay.banon)
 */
public class ClusterStateRequest extends MasterNodeOperationRequest {

    private boolean filterRoutingTable = false;

    private boolean filterNodes = false;

    private boolean filterMetaData = false;

    private String[] filteredIndices = Strings.EMPTY_ARRAY;

    public ClusterStateRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        return null;
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

    public String[] filteredIndices() {
        return filteredIndices;
    }

    public ClusterStateRequest filteredIndices(String... filteredIndices) {
        this.filteredIndices = filteredIndices;
        return this;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        filterRoutingTable = in.readBoolean();
        filterNodes = in.readBoolean();
        filterMetaData = in.readBoolean();
        int size = in.readVInt();
        if (size > 0) {
            filteredIndices = new String[size];
            for (int i = 0; i < filteredIndices.length; i++) {
                filteredIndices[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(filterRoutingTable);
        out.writeBoolean(filterNodes);
        out.writeBoolean(filterMetaData);
        out.writeVInt(filteredIndices.length);
        for (String filteredIndex : filteredIndices) {
            out.writeUTF(filteredIndex);
        }
    }
}
