/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A dedicated wrapper for exceptions encountered executing an ingest processor. The wrapper is needed as we currently only unwrap causes
 * for instances of {@link ElasticsearchWrapperException}.
 */
public class IngestProcessorException extends ElasticsearchException implements ElasticsearchWrapperException {

    IngestProcessorException(final Exception cause) {
        super(cause);
    }

    public IngestProcessorException(final StreamInput in) throws IOException {
        super(in);
    }

}
