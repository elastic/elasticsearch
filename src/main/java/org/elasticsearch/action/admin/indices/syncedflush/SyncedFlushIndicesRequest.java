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

package org.elasticsearch.action.admin.indices.syncedflush;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A synced flush request to sync flush one or more indices.
 * <p>Best created with {@link org.elasticsearch.client.Requests#flushRequest(String...)}.
 */
public class SyncedFlushIndicesRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private String[] indices;

    // timeout in ms
    long timeout = 30000;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

    SyncedFlushIndicesRequest() {
    }

    /**
     * Constructs a new synced flush request against one or more indices. If nothing is provided, all indices will
     * be sync flushed.
     */
    public SyncedFlushIndicesRequest(String... indices) {
        this.indices = indices;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        out.writeVLong(timeout);
        indicesOptions.writeIndicesOptions(out);
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "SyncedFlushIndicesRequest{" +
                "indices=" + Arrays.toString(indices) +
                ", timeout=" + timeout +
                '}';
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        timeout = in.readVLong();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public SyncedFlushIndicesRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @SuppressWarnings("unchecked")
    public final SyncedFlushIndicesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }
}
