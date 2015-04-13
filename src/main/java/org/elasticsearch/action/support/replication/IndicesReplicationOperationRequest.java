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

import java.io.IOException;

/**
 *
 */
public abstract class IndicesReplicationOperationRequest<T extends IndicesReplicationOperationRequest> extends ActionRequest<T> implements IndicesRequest.Replaceable {

    protected TimeValue timeout = ShardReplicationOperationRequest.DEFAULT_TIMEOUT;
    protected String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);

    protected WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    public TimeValue timeout() {
        return timeout;
    }

    protected IndicesReplicationOperationRequest() {
    }

    protected IndicesReplicationOperationRequest(ActionRequest actionRequest) {
        super(actionRequest);
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

    @Override
    public String[] indices() {
        return this.indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @SuppressWarnings("unchecked")
    public T indicesOptions(IndicesOptions indicesOptions) {
        if (indicesOptions == null) {
            throw new IllegalArgumentException("IndicesOptions must not be null");
        }
        this.indicesOptions = indicesOptions;
        return (T) this;
    }

    /**
     * The indices the request will execute against.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final T indices(String[] indices) {
        this.indices = indices;
        return (T) this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    @SuppressWarnings("unchecked")
    public final T consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        if (consistencyLevel == null) {
            throw new IllegalArgumentException("WriteConsistencyLevel must not be null");
        }
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
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        timeout = TimeValue.readTimeValue(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(consistencyLevel.id());
        timeout.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
