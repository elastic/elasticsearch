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

package org.elasticsearch.action.bench;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * Node-level response for the status of an on-going benchmark
 */
public class BenchmarkStatusNodeResponse extends ActionResponse implements Streamable, ToXContent {

    private String nodeName;
    private List<BenchmarkResponse> benchmarkResponses;

    public BenchmarkStatusNodeResponse() {
        benchmarkResponses = new ArrayList<>();
    }

    public void nodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String nodeName() {
        return nodeName;
    }

    public void addBenchResponse(BenchmarkResponse benchmarkResponse) {
        benchmarkResponses.add(benchmarkResponse);
    }

    public List<BenchmarkResponse> benchResponses() {
        return benchmarkResponses;
    }

    public int activeBenchmarks() {
        return benchResponses().size();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node", nodeName);
        builder.startArray("responses");
        for (BenchmarkResponse benchmarkResponse : benchmarkResponses) {
            benchmarkResponse.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodeName = in.readString();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            BenchmarkResponse br = new BenchmarkResponse();
            br.readFrom(in);
            benchmarkResponses.add(br);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeName);
        out.writeVInt(benchmarkResponses.size());
        for (BenchmarkResponse br : benchmarkResponses) {
            br.writeTo(out);
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
