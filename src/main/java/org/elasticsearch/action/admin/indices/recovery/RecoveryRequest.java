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

package org.elasticsearch.action.admin.indices.recovery;

import java.io.IOException;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * Request for recovery information
 */
public class RecoveryRequest extends BroadcastOperationRequest<RecoveryRequest> {

    private boolean detailed = false;       // Provides extra details in the response
    private boolean activeOnly = false;     // Only reports on active recoveries

    /**
     * Constructs a request for recovery information for all shards
     */
    public RecoveryRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    /**
     * Constructs a request for recovery information for all shards for the given indices
     *
     * @param indices   Comma-separated list of indices about which to gather recovery information
     */
    public RecoveryRequest(String... indices) {
        super(indices);
    }

    public boolean detailed() {
        return detailed;
    }

    public void detailed(boolean detailed) {
        this.detailed = detailed;
    }

    public boolean activeOnly() {
        return activeOnly;
    }

    public void activeOnly(boolean activeOnly) {
        this.activeOnly = activeOnly;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
        out.writeBoolean(activeOnly);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        detailed = in.readBoolean();
        activeOnly = in.readBoolean();
    }
}
