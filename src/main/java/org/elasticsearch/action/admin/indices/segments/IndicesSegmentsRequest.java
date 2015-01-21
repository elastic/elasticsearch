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

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class IndicesSegmentsRequest extends BroadcastOperationRequest<IndicesSegmentsRequest> {

    protected boolean verbose = false;
    
    public IndicesSegmentsRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public IndicesSegmentsRequest(String... indices) {
        super(indices);
    }

    /**
     * <code>true</code> if detailed information about each segment should be returned,
     * <code>false</code> otherwise.
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Sets the <code>verbose</code> option.
     * @see #verbose()
     */
    public void verbose(boolean v) {
        verbose = v;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(verbose);
        
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        verbose = in.readBoolean();
    }
    
}