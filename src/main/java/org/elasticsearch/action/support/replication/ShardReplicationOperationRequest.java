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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 *
 */
public abstract class ShardReplicationOperationRequest<T extends ShardReplicationOperationRequest> extends ActionRequest<T> implements IndicesRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    ShardId internalShardId;

    protected TimeValue timeout = DEFAULT_TIMEOUT;
    protected String index;

    private boolean threadedOperation = true;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;
    private volatile boolean canHaveDuplicates = false;

    protected ShardReplicationOperationRequest() {

    }

    /**
     * Creates a new request that inherits headers and context from the request provided as argument.
     */
    protected ShardReplicationOperationRequest(ActionRequest request) {
        super(request);
    }

    /**
     * Copy constructor that creates a new request that is a copy of the one provided as an argument.
     */
    protected ShardReplicationOperationRequest(T request) {
        this(request, request);
    }

    /**
     * Copy constructor that creates a new request that is a copy of the one provided as an argument.
     * The new request will inherit though headers and context from the original request that caused it.
     */
    protected ShardReplicationOperationRequest(T request, ActionRequest originalRequest) {
        super(originalRequest);
        this.timeout = request.timeout();
        this.index = request.index();
        this.threadedOperation = request.operationThreaded();
        this.consistencyLevel = request.consistencyLevel();
    }

    void setCanHaveDuplicates() {
        this.canHaveDuplicates = true;
    }

    /**
     * Is this request can potentially be dup on a single shard.
     */
    public boolean canHaveDuplicates() {
        return canHaveDuplicates;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public final boolean operationThreaded() {
        return threadedOperation;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    @SuppressWarnings("unchecked")
    public final T operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return (T) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    @SuppressWarnings("unchecked")
    public final T timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final T timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final T index(String index) {
        this.index = index;
        return (T) this;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    @SuppressWarnings("unchecked")
    public final T consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return (T) this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            internalShardId = ShardId.readShardId(in);
        }
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        timeout = TimeValue.readTimeValue(in);
        index = in.readString();
        canHaveDuplicates = in.readBoolean();
        // no need to serialize threaded* parameters, since they only matter locally
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStreamable(internalShardId);
        out.writeByte(consistencyLevel.id());
        timeout.writeTo(out);
        out.writeString(index);
        out.writeBoolean(canHaveDuplicates);
    }
}
