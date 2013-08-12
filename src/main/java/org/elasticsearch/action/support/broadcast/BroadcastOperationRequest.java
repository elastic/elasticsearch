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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public abstract class BroadcastOperationRequest<T extends BroadcastOperationRequest> extends ActionRequest<T> {

    protected String[] indices;

    private BroadcastOperationThreading operationThreading = BroadcastOperationThreading.THREAD_PER_SHARD;
    private IgnoreIndices ignoreIndices = IgnoreIndices.NONE;

    protected BroadcastOperationRequest() {

    }

    protected BroadcastOperationRequest(String[] indices) {
        this.indices = indices;
    }

    public String[] indices() {
        return indices;
    }

    @SuppressWarnings("unchecked")
    public final T indices(String... indices) {
        this.indices = indices;
        return (T) this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Controls the operation threading model.
     */
    public BroadcastOperationThreading operationThreading() {
        return operationThreading;
    }

    /**
     * Controls the operation threading model.
     */
    @SuppressWarnings("unchecked")
    public final T operationThreading(BroadcastOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return (T) this;
    }

    /**
     * Controls the operation threading model.
     */
    public T operationThreading(String operationThreading) {
        return operationThreading(BroadcastOperationThreading.fromString(operationThreading, this.operationThreading));
    }

    public IgnoreIndices ignoreIndices() {
        return ignoreIndices;
    }

    @SuppressWarnings("unchecked")
    public final T ignoreIndices(IgnoreIndices ignoreIndices) {
        this.ignoreIndices = ignoreIndices;
        return (T) this;
    }

    protected void beforeStart() {

    }

    protected void beforeLocalFork() {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        out.writeByte(operationThreading.id());
        out.writeByte(ignoreIndices.id());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        operationThreading = BroadcastOperationThreading.fromId(in.readByte());
        ignoreIndices = IgnoreIndices.fromId(in.readByte());
    }
}
