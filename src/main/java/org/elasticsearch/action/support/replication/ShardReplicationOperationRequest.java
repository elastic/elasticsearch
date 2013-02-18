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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 *
 */
public abstract class ShardReplicationOperationRequest<T extends ShardReplicationOperationRequest> extends ActionRequest<T> {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    protected String index;

    private boolean threadedOperation = true;
    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    protected ShardReplicationOperationRequest() {

    }

    public ShardReplicationOperationRequest(ActionRequest request) {
        super(request);
    }

    public ShardReplicationOperationRequest(T request) {
        super(request);
        this.timeout = request.getTimeout();
        this.index = request.getIndex();
        this.threadedOperation = request.isOperationThreaded();
        this.replicationType = request.getReplicationType();
        this.consistencyLevel = request.getConsistencyLevel();
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public final boolean isOperationThreaded() {
        return threadedOperation;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    @SuppressWarnings("unchecked")
    public final T setOperationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return (T) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    @SuppressWarnings("unchecked")
    public final T setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final T setTimeout(String timeout) {
        return setTimeout(TimeValue.parseTimeValue(timeout, null));
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public String getIndex() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final T setIndex(String index) {
        this.index = index;
        return (T) this;
    }

    /**
     * The replication type.
     */
    public ReplicationType getReplicationType() {
        return this.replicationType;
    }

    /**
     * Sets the replication type.
     */
    @SuppressWarnings("unchecked")
    public final T setReplicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return (T) this;
    }

    /**
     * Sets the replication type.
     */
    public final T setReplicationType(String replicationType) {
        return setReplicationType(ReplicationType.fromString(replicationType));
    }

    public WriteConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    @SuppressWarnings("unchecked")
    public final T setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
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
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        timeout = TimeValue.readTimeValue(in);
        index = in.readString();
        // no need to serialize threaded* parameters, since they only matter locally
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        timeout.writeTo(out);
        out.writeString(index);
    }

    /**
     * Called before the request gets forked into a local thread.
     */
    public void beforeLocalFork() {

    }
}
