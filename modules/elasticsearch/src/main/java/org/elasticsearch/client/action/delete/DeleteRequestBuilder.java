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

package org.elasticsearch.client.action.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.support.BaseRequestBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.VersionType;

/**
 * A delete document action request builder.
 *
 * @author kimchy (shay.banon)
 */
public class DeleteRequestBuilder extends BaseRequestBuilder<DeleteRequest, DeleteResponse> {

    public DeleteRequestBuilder(Client client, @Nullable String index) {
        super(client, new DeleteRequest(index));
    }

    /**
     * Sets the index the delete will happen on.
     */
    public DeleteRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the type of the document to delete.
     */
    public DeleteRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id of the document to delete.
     */
    public DeleteRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the delete request. Using this value to hash the shard
     * and not the id.
     */
    public DeleteRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public DeleteRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public DeleteRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the type of versioning to use. Defaults to {@link VersionType#INTERNAL}.
     */
    public DeleteRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public DeleteRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    public DeleteRequestBuilder setOperationThreaded(boolean threadedOperation) {
        request.operationThreaded(threadedOperation);
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public DeleteRequestBuilder setReplicationType(ReplicationType replicationType) {
        request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public DeleteRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    @Override protected void doExecute(ActionListener<DeleteResponse> listener) {
        client.delete(request, listener);
    }
}
