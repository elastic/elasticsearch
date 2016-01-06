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
package org.elasticsearch.action.ingest.simulate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SimulateDocumentVerboseResult implements SimulateDocumentResult<SimulateDocumentVerboseResult> {

    private static final SimulateDocumentVerboseResult PROTOTYPE = new SimulateDocumentVerboseResult(Collections.emptyList());

    private final List<SimulateProcessorResult> processorResults;

    public SimulateDocumentVerboseResult(List<SimulateProcessorResult> processorResults) {
        this.processorResults = processorResults;
    }

    public List<SimulateProcessorResult> getProcessorResults() {
        return processorResults;
    }

    public static SimulateDocumentVerboseResult readSimulateDocumentVerboseResultFrom(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    @Override
    public SimulateDocumentVerboseResult readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<SimulateProcessorResult> processorResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            processorResults.add(SimulateProcessorResult.readSimulateProcessorResultFrom(in));
        }
        return new SimulateDocumentVerboseResult(processorResults);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

    static final class Fields {
        static final XContentBuilderString PROCESSOR_RESULTS = new XContentBuilderString("processor_results");
    }
}
