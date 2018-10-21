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
package org.elasticsearch.action.ingest;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Holds the result of what a pipeline did to a sample document via the simulate api, but instead of {@link SimulateDocumentBaseResult}
 * this result class holds the intermediate result each processor did to the sample document.
 */
public final class SimulateDocumentVerboseResult implements SimulateDocumentResult {
    public static final String PROCESSOR_RESULT_FIELD = "processor_results";
    private final List<SimulateProcessorResult> processorResults;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SimulateDocumentVerboseResult, Void> PARSER =
        new ConstructingObjectParser<>(
            "simulate_document_verbose_result",
            true,
            a -> new SimulateDocumentVerboseResult((List<SimulateProcessorResult>)a[0])
        );
    static {
        PARSER.declareObjectArray(constructorArg(), SimulateProcessorResult.PARSER, new ParseField(PROCESSOR_RESULT_FIELD));
    }

    public SimulateDocumentVerboseResult(List<SimulateProcessorResult> processorResults) {
        this.processorResults = processorResults;
    }

    /**
     * Read from a stream.
     */
    public SimulateDocumentVerboseResult(StreamInput in) throws IOException {
        int size = in.readVInt();
        processorResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            processorResults.add(new SimulateProcessorResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(processorResults.size());
        for (SimulateProcessorResult result : processorResults) {
            result.writeTo(out);
        }
    }

    public List<SimulateProcessorResult> getProcessorResults() {
        return processorResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(PROCESSOR_RESULT_FIELD);
        for (SimulateProcessorResult processorResult : processorResults) {
            processorResult.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static SimulateDocumentVerboseResult fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
