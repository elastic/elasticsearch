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
package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.plugin.ingest.transport.TransportData;

import java.io.IOException;
import java.util.Objects;

public class SimulateProcessorResult implements Streamable, ToXContent {

    private String processorId;
    private TransportData data;
    private Throwable failure;

    public SimulateProcessorResult() {

    }

    public SimulateProcessorResult(String processorId, Data data) {
        this.processorId = processorId;
        this.data = new TransportData(data);
    }

    public SimulateProcessorResult(String processorId, Throwable failure) {
        this.processorId = processorId;
        this.failure = failure;
    }

    public boolean isFailed() {
        return this.failure != null;
    }

    public Data getData() {
        return data.get();
    }

    public String getProcessorId() {
        return processorId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boolean isFailure = in.readBoolean();
        if (isFailure) {
            this.failure = in.readThrowable();
        } else {
            this.data = new TransportData();
            this.data.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isFailed());
        if (isFailed()) {
            out.writeThrowable(failure);
        } else {
            out.writeString(processorId);
            data.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.PROCESSOR_ID, processorId);
        if (isFailed()) {
            ElasticsearchException.renderThrowable(builder, params, failure);
        } else {
            data.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimulateProcessorResult other = (SimulateProcessorResult) obj;
        return Objects.equals(processorId, other.processorId) && Objects.equals(data, other.data) && Objects.equals(failure, other.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processorId, data, failure);
    }

    static final class Fields {
        static final XContentBuilderString PROCESSOR_ID = new XContentBuilderString("processor_id");
    }
}
