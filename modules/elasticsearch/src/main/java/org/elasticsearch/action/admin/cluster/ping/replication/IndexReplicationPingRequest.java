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

package org.elasticsearch.action.admin.cluster.ping.replication;

import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class IndexReplicationPingRequest extends IndexReplicationOperationRequest {

    public IndexReplicationPingRequest(String index) {
        this.index = index;
    }

    IndexReplicationPingRequest(ReplicationPingRequest request, String index) {
        this.index = index;
        this.timeout = request.timeout();
        this.replicationType = request.replicationType();
    }

    IndexReplicationPingRequest() {
    }

    public IndexReplicationPingRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * The replication type to use with this operation.
     */
    public IndexReplicationPingRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }


    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}