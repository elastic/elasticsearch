/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.SimulateBulkRequest;

import java.util.Map;

/**
 * This is an implementation of IngestService that allows us to substitute pipeline definitions so that users can simulate ingest using
 * pipelines that they define on the fly.
 */
public class SimulateIngestService extends IngestService {
    private final Map<String, Pipeline> pipelineSubstitutions;

    public SimulateIngestService(IngestService ingestService, BulkRequest request) {
        super(ingestService);
        if (request instanceof SimulateBulkRequest simulateBulkRequest) {
            try {
                pipelineSubstitutions = simulateBulkRequest.getPipelineSubstitutions(ingestService);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            pipelineSubstitutions = Map.of();
        }
    }

    @Override
    public Pipeline getPipeline(String id) {
        Pipeline pipeline = pipelineSubstitutions.get(id);
        if (pipeline == null) {
            pipeline = super.getPipeline(id);
        }
        return pipeline;
    }
}
