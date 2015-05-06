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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A synced flush request to sync flush one or more indices. Here be a detailed description of what it does...
 * <p>Best created with {@link org.elasticsearch.client.Requests#flushRequest(String...)}.
 */
public class SyncedFlushRequest extends ActionRequest {

    private String[] indices;

    SyncedFlushRequest() {
    }

    /**
     * Constructs a new synced flush request against one or more indices. If nothing is provided, all indices will
     * be sync flushed.
     */
    public SyncedFlushRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices= in.readStringArray();
    }

    @Override
    public String toString() {
        return "SyncedFlushRequest{" +
                "indices=" + Arrays.toString(indices) +
                '}';
    }

    public void indices(String[] indices) {
        this.indices = indices;
    }

    public String[] indices() {
        return indices;
    }
}
