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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class IndicesStatusRequest extends BroadcastOperationRequest<IndicesStatusRequest> {

    private boolean recovery = false;

    private boolean snapshot = false;

    public IndicesStatusRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public IndicesStatusRequest(String... indices) {
        super(indices);
    }

    /**
     * Should the status include recovery information. Defaults to <tt>false</tt>.
     */
    public IndicesStatusRequest recovery(boolean recovery) {
        this.recovery = recovery;
        return this;
    }

    public boolean recovery() {
        return this.recovery;
    }

    /**
     * Should the status include recovery information. Defaults to <tt>false</tt>.
     */
    public IndicesStatusRequest snapshot(boolean snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public boolean snapshot() {
        return this.snapshot;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(recovery);
        out.writeBoolean(snapshot);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recovery = in.readBoolean();
        snapshot = in.readBoolean();
    }
}
