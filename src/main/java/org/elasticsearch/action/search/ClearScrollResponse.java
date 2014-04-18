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

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class ClearScrollResponse extends ActionResponse {

    private boolean succeeded;
    private boolean noneFreed;

    public ClearScrollResponse(boolean succeeded, boolean noneFreed) {
        this.succeeded = succeeded;
        this.noneFreed = noneFreed;
    }

    ClearScrollResponse() {
    }

    /**
     * @return Whether the attempt to clear a scroll was successful.
     */
    public boolean isSucceeded() {
        return succeeded;
    }

    /**
     * @return Whether no search contexts where freed. If this is <code>true</code> the assumption can be made,
     * that the scroll id specified in the request did not exist. (never existed, was expired, or completely consumed)
     */
    public boolean isNoneFreed() {
        return noneFreed;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        succeeded = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            noneFreed = in.readBoolean();
        } else {
            noneFreed = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(succeeded);
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeBoolean(noneFreed);
        }
    }
}
