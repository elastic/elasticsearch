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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * Benchmark status response
 */
public class BenchmarkStatusResponse extends ActionResponse implements Streamable, ToXContent {

    private String nodeName;
    private final List<BenchmarkResponse> benchmarkResponses = new ArrayList<>();
    private final List<String> errorMessages = new ArrayList<>();

    public BenchmarkStatusResponse() { }

    public BenchmarkStatusResponse(String nodeName) {
        this.nodeName = nodeName;
    }

    public void addBenchResponse(BenchmarkResponse response) {
        benchmarkResponses.add(response);
    }

    public List<BenchmarkResponse> benchmarkResponses() {
        return benchmarkResponses;
    }

    public String nodeName(String nodeName) {
        this.nodeName = nodeName;
        return nodeName;
    }

    public String nodeName() {
        return nodeName;
    }

    public void addErrors(List<String> errorMessages) {
        this.errorMessages.addAll(errorMessages);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (errorMessages.size() > 0) {
            builder.startArray("errors");
            for (String error : errorMessages) {
                builder.field(error);
            }
            builder.endArray();
        }

        if (benchmarkResponses.size() > 0) {
            builder.startObject("active_benchmarks");
            for (BenchmarkResponse benchmarkResponse : benchmarkResponses) {
                builder.startObject(benchmarkResponse.benchmarkName());
                benchmarkResponse.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }

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
        errorMessages.addAll(Arrays.asList(in.readStringArray()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeName);
        out.writeVInt(benchmarkResponses.size());
        for (BenchmarkResponse br : benchmarkResponses) {
            br.writeTo(out);
        }
        out.writeStringArray(errorMessages.toArray(new String[errorMessages.size()]));
    }
}
