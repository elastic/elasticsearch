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
package org.elasticsearch.action.support.single.custom;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 *
 */
public abstract class SingleCustomOperationRequest<T extends SingleCustomOperationRequest> extends ActionRequest<T> implements IndicesRequest {

    ShardId internalShardId;

    private boolean threadedOperation = true;
    private boolean preferLocal = true;
    private String index;

    protected SingleCustomOperationRequest() {
    }

    protected SingleCustomOperationRequest(ActionRequest request) {
        super(request);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public boolean operationThreaded() {
        return threadedOperation;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    @SuppressWarnings("unchecked")
    public final T operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return (T) this;
    }

    /**
     * if this operation hits a node with a local relevant shard, should it be preferred
     * to be executed on, or just do plain round robin. Defaults to <tt>true</tt>
     */
    @SuppressWarnings("unchecked")
    public final T preferLocal(boolean preferLocal) {
        this.preferLocal = preferLocal;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T index(String index) {
        this.index = index;
        return (T)this;
    }

    public String index() {
        return index;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public String[] indices() {
        if (index == null) {
            return Strings.EMPTY_ARRAY;
        }
        return new String[]{index};
    }

    /**
     * if this operation hits a node with a local relevant shard, should it be preferred
     * to be executed on, or just do plain round robin. Defaults to <tt>true</tt>
     */
    public boolean preferLocalShard() {
        return this.preferLocal;
    }

    public void beforeLocalFork() {

    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            internalShardId = ShardId.readShardId(in);
        }
        preferLocal = in.readBoolean();
        readIndex(in);
    }

    protected void readIndex(StreamInput in) throws IOException {
        index = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStreamable(internalShardId);
        out.writeBoolean(preferLocal);
        writeIndex(out);
    }

    protected void writeIndex(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
    }
}

