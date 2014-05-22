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

package org.elasticsearch.action.benchmark.status;

import org.elasticsearch.action.benchmark.BaseNodeActionResponse;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;

import java.io.IOException;

/**
 * Node-level response for a status request
 */
public class BenchmarkStatusNodeActionResponse extends BaseNodeActionResponse implements ToXContent {

    private BenchmarkStartResponse response;

    public BenchmarkStatusNodeActionResponse() { }

    public BenchmarkStatusNodeActionResponse(String benchmarkId, String nodeId) {
        super(benchmarkId, nodeId);
    }

    public BenchmarkStatusNodeActionResponse(String benchmarkId, String nodeId, BenchmarkStartResponse response) {
        super(benchmarkId, nodeId);
        this.response = response;
    }

    public void response(BenchmarkStartResponse response) {
        this.response = response;
    }

    public BenchmarkStartResponse response() {
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        response = new BenchmarkStartResponse();
        in.readOptionalStreamable(response);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStreamable(response);
    }
}
