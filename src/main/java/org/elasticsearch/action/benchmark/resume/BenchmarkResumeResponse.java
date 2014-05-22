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

package org.elasticsearch.action.benchmark.resume;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class BenchmarkResumeResponse extends ActionResponse implements ToXContent {

    private String benchmarkId;
    private Map<String, BenchmarkMetaData.Entry.NodeState> nodeResponses;

    public BenchmarkResumeResponse() {
       this(null);
    }

    public BenchmarkResumeResponse(final String benchmarkId) {
        this.benchmarkId = benchmarkId;
        this.nodeResponses = new ConcurrentHashMap<>();
    }

    public String getBenchmarkId() {
        return benchmarkId;
    }

    public void addNodeResponse(final String nodeId, final BenchmarkMetaData.Entry.NodeState nodeResponse) {
        nodeResponses.put(nodeId, nodeResponse);
    }

    public Map<String, BenchmarkMetaData.Entry.NodeState> getNodeResponses() {
        return ImmutableMap.copyOf(nodeResponses);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        benchmarkId = in.readString();
        int size = in.readVInt();
        nodeResponses = new ConcurrentHashMap<>(size);
        for (int i = 0; i < size; i++) {
            final String s = in.readString();
            final BenchmarkMetaData.Entry.NodeState ns = BenchmarkMetaData.Entry.NodeState.fromId(in.readByte());
            nodeResponses.put(s, ns);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(benchmarkId);
        out.writeVInt(nodeResponses.size());
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> ns : nodeResponses.entrySet()) {
            out.writeString(ns.getKey());
            out.writeByte(ns.getValue().id());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.BENCHMARK);
        builder.field(Fields.ID, benchmarkId);
        builder.startArray(Fields.NODES);
        for (Map.Entry<String, BenchmarkMetaData.Entry.NodeState> ns : nodeResponses.entrySet()) {
            builder.startObject();
            builder.field(Fields.NODE, ns.getKey());
            builder.field(Fields.STATE, ns.getValue());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString ID         = new XContentBuilderString("id");
        static final XContentBuilderString BENCHMARK  = new XContentBuilderString("benchmark");
        static final XContentBuilderString NODE       = new XContentBuilderString("node");
        static final XContentBuilderString NODES      = new XContentBuilderString("nodes");
        static final XContentBuilderString STATE      = new XContentBuilderString("state");
    }
}
