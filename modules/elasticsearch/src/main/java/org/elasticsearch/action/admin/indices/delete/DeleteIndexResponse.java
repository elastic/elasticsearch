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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;

/**
 * A response for a delete index action.
 *
 * @author kimchy (shay.banon)
 */
public class DeleteIndexResponse implements ActionResponse, Streamable {

    private boolean acknowledged;

    DeleteIndexResponse() {
    }

    DeleteIndexResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Has the index deletion been acknowledged by all current cluster nodes within the
     * provided {@link DeleteIndexRequest#timeout(org.elasticsearch.util.TimeValue)}.
     */
    public boolean acknowledged() {
        return acknowledged;
    }

    /**
     * Has the index deletion been acknowledged by all current cluster nodes within the
     * provided {@link DeleteIndexRequest#timeout(org.elasticsearch.util.TimeValue)}.
     */
    public boolean getAcknowledged() {
        return acknowledged();
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        acknowledged = in.readBoolean();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }
}
