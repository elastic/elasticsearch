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
import java.util.ArrayList;
import java.util.List;

/**
 * Node-level response for the status of a benchmark abort request
 */
public class AbortBenchmarkNodeResponse extends ActionResponse implements Streamable, ToXContent {

    private String nodeName;
    private List<AbortBenchmarkNodeStatus> abortBenchmarkNodeStatuses = new ArrayList<>();

    public static class AbortBenchmarkNodeStatus implements Streamable, ToXContent {
        String benchmarkName;
        String errorMessage;
        boolean aborted;

        public AbortBenchmarkNodeStatus() { }

        public AbortBenchmarkNodeStatus(String benchmarkName, boolean aborted) {
            this(benchmarkName, "", aborted);
        }

        public AbortBenchmarkNodeStatus(String benchmarkName, String errorMessage, boolean aborted) {
            this.benchmarkName = benchmarkName;
            this.errorMessage = errorMessage;
            this.aborted = aborted;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            benchmarkName = in.readString();
            errorMessage = in.readString();
            aborted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(benchmarkName);
            out.writeString(errorMessage);
            out.writeBoolean(aborted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.BENCHMARK, benchmarkName);
            if (errorMessage != null && !errorMessage.isEmpty()) {
                builder.field(Fields.ERR, errorMessage);
            }
            builder.field(Fields.ABORTED, aborted);
            builder.endObject();
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString BENCHMARK = new XContentBuilderString("benchmark");
            static final XContentBuilderString ERR = new XContentBuilderString("error");
            static final XContentBuilderString ABORTED = new XContentBuilderString("aborted");
        }
    }

    public AbortBenchmarkNodeResponse() { }

    public AbortBenchmarkNodeResponse(String nodeName) {
        this.nodeName = nodeName;
    }

    public void add(String benchmarkName, String error, boolean aborted) {
        abortBenchmarkNodeStatuses.add(new AbortBenchmarkNodeStatus(benchmarkName, error, aborted));
    }

    public void add(String benchmarkName, boolean aborted) {
        abortBenchmarkNodeStatuses.add(new AbortBenchmarkNodeStatus(benchmarkName, aborted));
    }

    public List<AbortBenchmarkNodeStatus> abortBenchmarkNodeStatuses() {
        return abortBenchmarkNodeStatuses;
    }

    public void nodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String nodeName() {
        return nodeName;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodeName = in.readString();
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            AbortBenchmarkNodeStatus status = new AbortBenchmarkNodeStatus();
            status.readFrom(in);
            abortBenchmarkNodeStatuses.add(status);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeName);
        out.writeVInt(abortBenchmarkNodeStatuses.size());
        for (AbortBenchmarkNodeStatus status : abortBenchmarkNodeStatuses) {
            status.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.NODE, nodeName);
        builder.startArray();
        for (AbortBenchmarkNodeStatus status : abortBenchmarkNodeStatuses) {
           status.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString NODE = new XContentBuilderString("node");
    }
}
