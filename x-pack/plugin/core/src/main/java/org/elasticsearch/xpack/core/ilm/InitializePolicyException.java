package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;

/**
 * Exception thrown when a problem is encountered while initialising an ILM policy for an index.
 */
public class InitializePolicyException extends ElasticsearchException {

    public InitializePolicyException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
