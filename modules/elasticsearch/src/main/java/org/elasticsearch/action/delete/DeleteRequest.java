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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;

import java.io.IOException;

import static org.elasticsearch.action.Actions.*;

/**
 * A request to delete a document from an index based on its type and id. Best created using
 * {@link org.elasticsearch.client.Requests#deleteRequest(String)}.
 *
 * <p>The operation requires the {@link #index()}, {@link #type(String)} and {@link #id(String)} to
 * be set.
 *
 * @author kimchy (shay.banon)
 * @see DeleteResponse
 * @see org.elasticsearch.client.Client#delete(DeleteRequest)
 * @see org.elasticsearch.client.Requests#deleteRequest(String)
 */
public class DeleteRequest extends ShardReplicationOperationRequest {

    private String type;
    private String id;
    @Nullable private String routing;
    private boolean refresh;
    private long version;
    private VersionType versionType = VersionType.INTERNAL;

    /**
     * Constructs a new delete request against the specified index. The {@link #type(String)} and {@link #id(String)}
     * must be set.
     */
    public DeleteRequest(String index) {
        this.index = index;
    }

    /**
     * Constructs a new delete request against the specified index with the type and id.
     *
     * @param index The index to get the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    public DeleteRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public DeleteRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the index the delete will happen on.
     */
    @Override public DeleteRequest index(String index) {
        super.index(index);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public DeleteRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    @Override public DeleteRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    @Override public DeleteRequest replicationType(ReplicationType replicationType) {
        super.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    @Override public DeleteRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        super.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * The type of the document to delete.
     */
    public String type() {
        return type;
    }

    /**
     * Sets the type of the document to delete.
     */
    @Required public DeleteRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the document to delete.
     */
    public String id() {
        return id;
    }

    /**
     * Sets the id of the document to delete.
     */
    @Required public DeleteRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public DeleteRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public DeleteRequest parent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public DeleteRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Controls the shard routing of the delete request. Using this value to hash the shard
     * and not the id.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public DeleteRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public DeleteRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public DeleteRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readUTF();
        id = in.readUTF();
        if (in.readBoolean()) {
            routing = in.readUTF();
        }
        refresh = in.readBoolean();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(type);
        out.writeUTF(id);
        if (routing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(routing);
        }
        out.writeBoolean(refresh);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
    }

    @Override public String toString() {
        return "delete {[" + index + "][" + type + "][" + id + "]}";
    }
}