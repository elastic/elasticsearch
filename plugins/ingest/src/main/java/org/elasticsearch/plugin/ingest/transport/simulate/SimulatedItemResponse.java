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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SimulatedItemResponse implements Streamable, ToXContent {

    private Data data;
    private List<ProcessorResult> processorResultList;
    private Throwable failure;

    public SimulatedItemResponse() {

    }

    public SimulatedItemResponse(Data data) {
        this.data = data;
    }

    public SimulatedItemResponse(List<ProcessorResult> processorResultList) {
        this.processorResultList = processorResultList;
    }

    public SimulatedItemResponse(Throwable failure) {
        this.failure = failure;
    }

    public boolean isFailed() {
        if (failure != null) {
            return true;
        } else if (processorResultList != null) {
            for (ProcessorResult result : processorResultList) {
                if (result.isFailed()) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean isVerbose() {
        return this.processorResultList != null;
    }

    public Data getData() {
        return data;
    }

    public List<ProcessorResult> getProcessorResultList() {
        return processorResultList;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boolean isFailed = in.readBoolean();
        boolean isVerbose = in.readBoolean();
        if (isFailed) {
            this.failure = in.readThrowable();
        } else if (isVerbose) {
            int size = in.readVInt();
            processorResultList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ProcessorResult processorResult = new ProcessorResult();
                processorResult.readFrom(in);
                processorResultList.add(processorResult);
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

        if (failure != null) {
            out.writeThrowable(failure);
        } else if (isVerbose()) {
            out.writeVInt(processorResultList.size());
            for (ProcessorResult p : processorResultList) {
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
        if (failure != null) {
            builder.field(Fields.ERROR_MESSAGE, ExceptionsHelper.detailedMessage(failure));
        } else if (isVerbose()) {
            builder.startArray(Fields.PROCESSOR_RESULTS);
            for (ProcessorResult processorResult : processorResultList) {
                builder.value(processorResult);
            }
            builder.endArray();
        } else {
            builder.field(Fields.MODIFIED, data.isModified());
            builder.field(Fields.DOCUMENT, data.asMap());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) { return true; }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimulatedItemResponse other = (SimulatedItemResponse) obj;
        return Objects.equals(data, other.data) && Objects.equals(processorResultList, other.processorResultList) && Objects.equals(failure, other.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, processorResultList, failure);
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENT = new XContentBuilderString("doc");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString ERROR_MESSAGE = new XContentBuilderString("error_message");
        static final XContentBuilderString MODIFIED = new XContentBuilderString("modified");
        static final XContentBuilderString PROCESSOR_RESULTS = new XContentBuilderString("processor_results");
    }
}
