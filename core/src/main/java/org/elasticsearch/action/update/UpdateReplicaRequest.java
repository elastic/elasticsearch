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

package org.elasticsearch.action.update;

import org.elasticsearch.action.DocumentWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.VersionType;

import java.io.IOException;

/** Replica request for update operation holds translated (index/delete) requests */
public class UpdateReplicaRequest extends DocumentWriteRequest<UpdateReplicaRequest> {
    private DocumentWriteRequest<?> request;

    public UpdateReplicaRequest() {
    }

    public UpdateReplicaRequest(DocumentWriteRequest<?> request) {
        assert !(request instanceof UpdateReplicaRequest) : "underlying request must not be a update replica request";
        this.request = request;
        this.index = request.index();
        setRefreshPolicy(request.getRefreshPolicy());
        setShardId(request.shardId());
        setParentTask(request.getParentTask());
    }

    public DocumentWriteRequest<?> getRequest() {
        return request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request = DocumentWriteRequest.readDocumentRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        DocumentWriteRequest.writeDocumentRequest(out, request);
    }

    @Override
    public String type() {
        return request.type();
    }

    @Override
    public String id() {
        return request.id();
    }

    @Override
    public UpdateReplicaRequest routing(String routing) {
        throw new UnsupportedOperationException("setting routing is not supported");
    }

    @Override
    public String routing() {
        return request.routing();
    }

    @Override
    public String parent() {
        return request.parent();
    }

    @Override
    public long version() {
        return request.version();
    }

    @Override
    public UpdateReplicaRequest version(long version) {
        throw new UnsupportedOperationException("setting version is not supported");
    }

    @Override
    public VersionType versionType() {
        return request.versionType();
    }

    @Override
    public UpdateReplicaRequest versionType(VersionType versionType) {
        throw new UnsupportedOperationException("setting version type is not supported");
    }

    @Override
    public OpType opType() {
        return request.opType();
    }
}
