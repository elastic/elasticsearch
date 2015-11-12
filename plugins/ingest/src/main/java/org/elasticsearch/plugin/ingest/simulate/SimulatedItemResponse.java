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
package org.elasticsearch.plugin.ingest.simulate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SimulatedItemResponse implements Streamable, StatusToXContent {

    private Data data;
    private List<ProcessedData> processedDataList;
    private Throwable failure;

    public SimulatedItemResponse() {

    }

    public SimulatedItemResponse(Data data) {
        this.data = data;
    }

    public SimulatedItemResponse(List<ProcessedData> processedDataList) {
        this.processedDataList = processedDataList;
    }

    public SimulatedItemResponse(Throwable failure) {
        this.failure = failure;
    }

    public boolean isFailed() {
        return this.failure != null;
    }

    public boolean isVerbose() {
        return this.processedDataList != null;
    }

    public Data getData() {
        return data;
    }

    public List<ProcessedData> getProcessedDataList() {
        return processedDataList;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boolean isFailed = in.readBoolean();
        boolean isVerbose = in.readBoolean();
        if (isFailed) {
            this.failure = in.readThrowable();
            // TODO(talevy): check out mget for throwable limitations
        } else if (isVerbose) {
            int size = in.readVInt();
            processedDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ProcessedData processedData = new ProcessedData();
                processedData.readFrom(in);
                processedDataList.add(processedData);
            }
        } else {
            String index = in.readString();
            String type = in.readString();
            String id = in.readString();
            Map<String, Object> doc = in.readMap();
            this.data = new Data(index, type, id, doc);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isFailed());
        out.writeBoolean(isVerbose());

        if (isFailed()) {
            out.writeThrowable(failure);
        } else if (isVerbose()) {
            out.writeVInt(processedDataList.size());
            for (ProcessedData p : processedDataList) {
                p.writeTo(out);
            }
        } else {
            out.writeString(data.getIndex());
            out.writeString(data.getType());
            out.writeString(data.getId());
            out.writeMap(data.getDocument());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.ERROR, isFailed());
        builder.field(Fields.VERBOSE, isVerbose());
        if (isFailed()) {
            builder.field(Fields.FAILURE, failure.toString());
        } else if (isVerbose()) {
            builder.startArray(Fields.PROCESSOR_STEPS);
            for (ProcessedData processedData : processedDataList) {
                builder.value(processedData);
            }
            builder.endArray();
        } else {
            builder.field(Fields.MODIFIED, data.isModified());
            builder.field(Fields.DOCUMENT, data.getDocument());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public RestStatus status() {
        if (isFailed()) {
            return RestStatus.BAD_REQUEST;
        } else {
            return RestStatus.OK;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) { return true; }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimulatedItemResponse other = (SimulatedItemResponse) obj;
        return Objects.equals(data, other.data) && Objects.equals(processedDataList, other.processedDataList) && Objects.equals(failure, other.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, processedDataList, failure);
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENT = new XContentBuilderString("doc");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString VERBOSE = new XContentBuilderString("verbose");
        static final XContentBuilderString FAILURE = new XContentBuilderString("failure");
        static final XContentBuilderString MODIFIED = new XContentBuilderString("modified");
        static final XContentBuilderString PROCESSOR_STEPS = new XContentBuilderString("processor_steps");
    }
}
