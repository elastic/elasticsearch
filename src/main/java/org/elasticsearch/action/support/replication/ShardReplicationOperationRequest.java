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
public abstract class ShardReplicationOperationRequest implements ActionRequest {


    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    protected String index;

    private boolean threadedListener = false;
    private boolean threadedOperation = true;
    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    public ShardReplicationOperationRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override
    public boolean listenerThreaded() {
        return threadedListener;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override
    public ShardReplicationOperationRequest listenerThreaded(boolean threadedListener) {
        this.threadedListener = threadedListener;
        return this;
    }


    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public boolean operationThreaded() {
        return threadedOperation;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally. Defaults
     * to <tt>true</tt> when running in embedded mode.
     */
    public ShardReplicationOperationRequest operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return this;
    }

    /**
     * The replication type.
     */
    public ReplicationType replicationType() {
        return this.replicationType;
    }

    /**
     * Sets the replication type.
     */
    public ShardReplicationOperationRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    public ShardReplicationOperationRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
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
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        timeout = TimeValue.readTimeValue(in);
        index = in.readUTF();
        // no need to serialize threaded* parameters, since they only matter locally
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        timeout.writeTo(out);
        out.writeUTF(index);
    }

    /**
     * Called before the request gets forked into a local thread.
     */
    public void beforeLocalFork() {

    }
}
