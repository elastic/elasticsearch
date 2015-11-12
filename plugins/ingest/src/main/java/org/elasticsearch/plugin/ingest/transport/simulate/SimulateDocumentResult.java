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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SimulateDocumentResult implements Streamable, ToXContent {

    private TransportData data;
    private List<SimulateProcessorResult> processorResultList;
    private Throwable failure;

    public SimulateDocumentResult() {

    }

    public SimulateDocumentResult(Data data) {
        this.data = new TransportData(data);
    }

    public SimulateDocumentResult(List<SimulateProcessorResult> processorResultList) {
        this.processorResultList = processorResultList;
    }

    public SimulateDocumentResult(Throwable failure) {
        this.failure = failure;
    }

    public boolean isFailed() {
        if (failure != null) {
            return true;
        }
        return false;
    }

    public boolean isVerbose() {
        return this.processorResultList != null;
    }

    public Data getData() {
        return data.get();
    }

    public List<SimulateProcessorResult> getProcessorResultList() {
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
                SimulateProcessorResult processorResult = new SimulateProcessorResult();
                processorResult.readFrom(in);
                processorResultList.add(processorResult);
            }
        } else {
            this.data = new TransportData();
            this.data.readFrom(in);
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
            for (SimulateProcessorResult p : processorResultList) {
                p.writeTo(out);
            }
        } else {
            data.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (isFailed()) {
            ElasticsearchException.renderThrowable(builder, params, failure);
        } else if (isVerbose()) {
            builder.startArray(Fields.PROCESSOR_RESULTS);
            for (SimulateProcessorResult processorResult : processorResultList) {
                processorResult.toXContent(builder, params);
            }
            builder.endArray();
        } else {
            data.toXContent(builder, params);
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
        SimulateDocumentResult other = (SimulateDocumentResult) obj;
        return Objects.equals(data, other.data) && Objects.equals(processorResultList, other.processorResultList) && Objects.equals(failure, other.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, processorResultList, failure);
    }

    static final class Fields {
        static final XContentBuilderString PROCESSOR_RESULTS = new XContentBuilderString("processor_results");
    }
}
