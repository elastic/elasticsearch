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

package org.elasticsearch.action.benchmark;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Base class for action response payloads.
 */
public abstract class BaseNodeActionResponse extends ActionResponse implements ToXContent {

    public String nodeId;
    public String benchmarkId;
    public String error;

    public BaseNodeActionResponse() { }

    public BaseNodeActionResponse(final String benchmarkId, final String nodeId) {
        this.benchmarkId = benchmarkId;
        this.nodeId      = nodeId;
    }

    public void error(String error) {
        this.error = error;
    }

    public String error() {
        return error;
    }

    public boolean hasErrors() {
        return !(error == null || error.isEmpty());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        benchmarkId = in.readString();
        nodeId      = in.readString();
        error       = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(benchmarkId);
        out.writeString(nodeId);
        out.writeOptionalString(error);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.BENCHMARK_ID, benchmarkId);
        builder.field(Fields.NODE_ID, nodeId);
        if (error != null && !error.isEmpty()) {
            builder.field(Fields.ERROR, error);
        }
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString NODE_ID      = new XContentBuilderString("node_id");
        static final XContentBuilderString BENCHMARK_ID = new XContentBuilderString("benchmark_id");
        static final XContentBuilderString ERROR        = new XContentBuilderString("error");
    }
}
