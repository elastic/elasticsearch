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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 *
 */
public class IndicesReplicationOperationRequest<T extends IndicesReplicationOperationRequest> extends ActionRequest<T> {

    protected TimeValue timeout = ShardReplicationOperationRequest.DEFAULT_TIMEOUT;
    protected String[] indices;
    protected ReplicationType replicationType = ReplicationType.DEFAULT;
    protected WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    public TimeValue timeout() {
        return timeout;
    }

    /**
     * A timeout to wait if the delete by query operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    @SuppressWarnings("unchecked")
    public final T timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    /**
     * A timeout to wait if the delete by query operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    @SuppressWarnings("unchecked")
    public T timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null);
        return (T) this;
    }

    public String[] indices() {
        return this.indices;
    }

    /**
     * The indices the request will execute against.
     */
    @SuppressWarnings("unchecked")
    public final T indices(String[] indices) {
        this.indices = indices;
        return (T) this;
    }

    public ReplicationType replicationType() {
        return this.replicationType;
    }

    /**
     * Sets the replication type.
     */
    @SuppressWarnings("unchecked")
    public final T replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return (T) this;
    }

    /**
     * Sets the replication type.
     */
    public final T replicationType(String replicationType) {
        return replicationType(ReplicationType.fromString(replicationType));
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
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        timeout = TimeValue.readTimeValue(in);
        indices = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        timeout.writeTo(out);
        out.writeStringArrayNullable(indices);
    }
}