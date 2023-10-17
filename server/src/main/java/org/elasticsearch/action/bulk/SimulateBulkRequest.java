/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This extends BulkRequest with support for providing substitute pipeline definitions.
 */
public class SimulateBulkRequest extends BulkRequest {
    // Non-private for unit testing
    Map<String, Object> pipelineSubstitutions = Map.of();

    public SimulateBulkRequest() {
        super();
    }

    public SimulateBulkRequest(StreamInput in) throws IOException {
        super(in);
        this.pipelineSubstitutions = in.readMap();
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(pipelineSubstitutions);
    }

    public void setPipelineSubstitutions(Map<String, Object> pipelineSubstitutions) {
        this.pipelineSubstitutions = pipelineSubstitutions;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Pipeline> getPipelineSubstitutions(IngestService ingestService) throws Exception {
        Map<String, Pipeline> parsedPipelineSubstitutions = new HashMap<>();
        if (pipelineSubstitutions != null) {
            for (Map.Entry<String, Object> entry : pipelineSubstitutions.entrySet()) {
                String pipelineId = entry.getKey();
                Pipeline pipeline = Pipeline.create(
                    pipelineId,
                    (Map<String, Object>) entry.getValue(),
                    ingestService.getProcessorFactories(),
                    ingestService.getScriptService()
                );
                parsedPipelineSubstitutions.put(pipelineId, pipeline);
            }
        }
        return parsedPipelineSubstitutions;
    }
}
