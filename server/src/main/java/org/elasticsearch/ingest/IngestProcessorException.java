package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;

/**
 * A dedicated wrapper for exceptions encountered executing an ingest processor. The wrapper is needed as we currently only unwrap causes
 * for instances of {@link ElasticsearchWrapperException}.
 */
class IngestProcessorException extends ElasticsearchException implements ElasticsearchWrapperException {

    IngestProcessorException(final Exception cause) {
        super(cause);
    }

}
