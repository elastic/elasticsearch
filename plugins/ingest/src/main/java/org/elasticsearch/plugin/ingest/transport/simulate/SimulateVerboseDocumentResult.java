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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SimulateVerboseDocumentResult extends SimulateDocumentResult {
    public static final int STREAM_ID = 1;

    private List<SimulateProcessorResult> processorResults;

    public SimulateVerboseDocumentResult() {

    }

    public SimulateVerboseDocumentResult(List<SimulateProcessorResult> processorResults) {
        this.processorResults = processorResults;
    }

    @Override
    public int getStreamId() {
        return STREAM_ID;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int streamId = in.readVInt();
        if (streamId != STREAM_ID) {
            throw new IOException("stream_id [" + streamId + "] does not match " + getClass().getName() + " [stream_id=" + STREAM_ID + "]");
        }
        int size = in.readVInt();
        processorResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SimulateProcessorResult processorResult = new SimulateProcessorResult();
            processorResult.readFrom(in);
            processorResults.add(processorResult);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(STREAM_ID);
        out.writeVInt(processorResults.size());
        for (SimulateProcessorResult result : processorResults) {
            result.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.PROCESSOR_RESULTS);
        for (SimulateProcessorResult processorResult : processorResults) {
            processorResult.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimulateVerboseDocumentResult that = (SimulateVerboseDocumentResult) o;
        return Objects.equals(processorResults, that.processorResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processorResults);
    }

    static final class Fields {
        static final XContentBuilderString PROCESSOR_RESULTS = new XContentBuilderString("processor_results");
    }
}
