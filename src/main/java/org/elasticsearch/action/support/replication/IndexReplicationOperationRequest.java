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

import org.elasticsearch.action.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Request used within {@link org.elasticsearch.action.support.replication.TransportIndexReplicationOperationAction}.
 * Since the corresponding action is internal that gets always executed locally, this request never gets sent over the transport.
 * The specified index is expected to be a concrete index. Relies on input validation done by the caller actions.
 */
public abstract class IndexReplicationOperationRequest<T extends IndexReplicationOperationRequest> extends ActionRequest<T> implements IndicesRequest {

    private final TimeValue timeout;
    private final String index;
    private final WriteConsistencyLevel consistencyLevel;
    private final OriginalIndices originalIndices;

    protected IndexReplicationOperationRequest(String index, TimeValue timeout, WriteConsistencyLevel consistencyLevel,
                                               String[] originalIndices, IndicesOptions originalIndicesOptions, ActionRequest request) {
        super(request);
        this.index = index;
        this.timeout = timeout;
        this.consistencyLevel = consistencyLevel;
        this.originalIndices = new OriginalIndices(originalIndices, originalIndicesOptions);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("IndexReplicationOperationRequest is not supposed to be sent over the transport");
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("IndexReplicationOperationRequest is not supposed to be sent over the transport");
    }
}
