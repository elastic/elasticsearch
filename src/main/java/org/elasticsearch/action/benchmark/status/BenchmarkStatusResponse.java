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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class BenchmarkStatusResponse extends ActionResponse implements ToXContent {

    private Map<String, List<BenchmarkStatusNodeActionResponse>> responses;
    private List<String>                                   errors;

    public BenchmarkStatusResponse() {
        responses = new ConcurrentHashMap<>();
        errors    = new ArrayList<>();
    }

    public void addNodeResponse(BenchmarkStatusNodeActionResponse nodeResponse) {

        List<BenchmarkStatusNodeActionResponse> newList = new CopyOnWriteArrayList<>();
        List<BenchmarkStatusNodeActionResponse> list    = responses.putIfAbsent(nodeResponse.benchmarkId, newList);
        if (list != null) {
            list.add(nodeResponse);
        } else {
            newList.add(nodeResponse);
        }
    }

    public List<String> errors() {
        return this.errors;
    }

    public void error(String error) {
        errors.add(error);
    }

    public void errors(List<String> errors) {
        this.errors.addAll(errors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            final String benchmarkId = in.readString();
            final List<BenchmarkStatusNodeActionResponse> list = new CopyOnWriteArrayList<>();
            final int s = in.readVInt();
            for (int j = 0; j < s; j++) {
                BenchmarkStatusNodeActionResponse b = new BenchmarkStatusNodeActionResponse();
                b.readFrom(in);
                list.add(b);
            }
            responses.put(benchmarkId, list);
        }
        size = in.readVInt();
        errors = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            errors.add(in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(responses.size());
        for (Map.Entry<String, List<BenchmarkStatusNodeActionResponse>> entry : responses.entrySet()) {
            out.writeString(entry.getKey());
            final List<BenchmarkStatusNodeActionResponse> list = entry.getValue();
            out.writeVInt(list == null ? 0 : list.size());
            if (list != null) {
                for (BenchmarkStatusNodeActionResponse nodeResponse : list) {
                    nodeResponse.writeTo(out);
                }
            }
        }
        out.writeVInt(errors.size());
        for (String s : errors) {
            out.writeString(s);
        }
    }
}
