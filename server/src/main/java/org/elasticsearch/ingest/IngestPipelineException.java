/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.ingest.CompoundProcessor.PIPELINE_ORIGIN_EXCEPTION_HEADER;

/**
 * A dedicated wrapper for exceptions encountered while executing an ingest pipeline. Unlike {@link IngestProcessorException}, this
 * exception indicates an issue with the overall pipeline execution, either due to mid-process validation problem or other non-processor
 * level issues with the execution. The wrapper is needed as we currently only unwrap causes for instances of
 * {@link ElasticsearchWrapperException}.
 */
public class IngestPipelineException extends ElasticsearchException implements ElasticsearchWrapperException {

    IngestPipelineException(final String pipeline, final Exception cause) {
        super(cause);
        this.addHeader(PIPELINE_ORIGIN_EXCEPTION_HEADER, List.of(pipeline));
    }

    public IngestPipelineException(final StreamInput in) throws IOException {
        super(in);
    }

}
