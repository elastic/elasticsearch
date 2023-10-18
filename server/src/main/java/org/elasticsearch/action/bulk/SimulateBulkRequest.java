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

import java.io.IOException;
import java.util.Map;

/**
 * This extends BulkRequest with support for providing substitute pipeline definitions.
 */
public class SimulateBulkRequest extends BulkRequest {
    private Map<String, Object> pipelineSubstitutions = Map.of();

    public SimulateBulkRequest() {
        super();
    }

    public SimulateBulkRequest(StreamInput in) throws IOException {
        super(in);
        this.pipelineSubstitutions = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(pipelineSubstitutions);
    }

    /**
     * This sets the pipeline definitions that are to be used in place of any pre-existing pipeline definitions with the same pipelineId.
     * The key of the map is the pipelineId, and the value the pipeline definition as parsed by XContentHelper.convertToMap().
     * @param pipelineSubstitutions
     */
    public void setPipelineSubstitutions(Map<String, Object> pipelineSubstitutions) {
        this.pipelineSubstitutions = pipelineSubstitutions;
    }

    public Map<String, Object> getPipelineSubstitutions() {
        return pipelineSubstitutions;
    }
}
