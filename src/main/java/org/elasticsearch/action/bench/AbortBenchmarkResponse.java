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
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * Response for a benchmark abort request
 */
public class AbortBenchmarkResponse extends ActionResponse implements Streamable, ToXContent {

    private String benchmarkName;
    private String errorMessage;
    private List<AbortBenchmarkNodeResponse> nodeResponses = new ArrayList<>();

    AbortBenchmarkResponse() {
        super();
    }

    AbortBenchmarkResponse(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    AbortBenchmarkResponse(String benchmarkName, String errorMessage) {
        this.benchmarkName = benchmarkName;
        this.errorMessage = errorMessage;
    }

    public void addNodeResponse(AbortBenchmarkNodeResponse nodeResponse) {
        nodeResponses.add(nodeResponse);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkName = in.readString();
        errorMessage = in.readString();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            AbortBenchmarkNodeResponse nodeResponse = new AbortBenchmarkNodeResponse();
            nodeResponse.readFrom(in);
            nodeResponses.add(nodeResponse);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(benchmarkName);
        out.writeString(errorMessage);
        out.writeVInt(nodeResponses.size());
        for (AbortBenchmarkNodeResponse nodeResponse : nodeResponses) {
            nodeResponse.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (errorMessage != null && !errorMessage.isEmpty()) {
            builder.field(Fields.ERROR, errorMessage);
        }
        if (nodeResponses.size() > 0) {
            builder.startArray(Fields.ABORTED);
            for (AbortBenchmarkNodeResponse nodeResponse : nodeResponses) {
                nodeResponse.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString ABORTED = new XContentBuilderString("aborted_benchmarks");
    }
}
