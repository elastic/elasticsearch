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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BenchmarkStatusResponses extends ActionResponse implements ToXContent {

    private List<BenchmarkStartResponse> responses = new ArrayList<>();

    public BenchmarkStatusResponses() { }

    public BenchmarkStatusResponses(List<BenchmarkStartResponse> responses) {
        this.responses = responses;
    }

    public void add(BenchmarkStartResponse response) {
        responses.add(response);
    }

    public List<BenchmarkStartResponse> responses() {
        return responses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        final int size = in.readVInt();
        responses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            BenchmarkStartResponse response = new BenchmarkStartResponse();
            response.readFrom(in);
            responses.add(response);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(responses.size());
        for (BenchmarkStartResponse response : responses) {
            response.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.BENCHMARKS);
        for (BenchmarkStartResponse response : responses) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString BENCHMARKS = new XContentBuilderString("benchmarks");
    }
}
