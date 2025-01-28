/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds the result of what a pipeline did to a sample document via the simulate api, but instead of {@link SimulateDocumentBaseResult}
 * this result class holds the intermediate result each processor did to the sample document.
 */
public final class SimulateDocumentVerboseResult implements SimulateDocumentResult {
    public static final String PROCESSOR_RESULT_FIELD = "processor_results";
    private final List<SimulateProcessorResult> processorResults;

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
        out.writeCollection(processorResults);
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
}
