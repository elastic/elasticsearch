/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;

/**
 * An Abstract Processor that holds tag and description information
 * about the processor.
 */
public abstract class AbstractProcessor implements Processor {
    protected final String tag;
    protected final String description;

    protected AbstractProcessor(String tag, String description) {
        this.tag = tag;
        this.description = description;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Helper method to be used by processors that need to catch and log Throwables.
     * <p>
     * If trace logging is enabled, then we log the provided message and the full stacktrace
     * On the other hand if trace logging isn't enabled, then we log the provided message and the message from the Throwable (but not a
     * stacktrace).
     * <p>
     * Regardless of the logging level, we throw an ElasticsearchException that has the context in its message
     *
     * @param message A message to be logged and to be included in the message of the returned ElasticsearchException
     * @param throwable The Throwable that has been caught
     * @return A new ElasticsearchException whose message includes the passed-in message and the message from the passed-in Throwable. It
     * will not however wrap the given Throwable.
     */
    protected ElasticsearchException logAndBuildException(String message, Throwable throwable) {
        String cause = throwable.getClass().getName();
        if (throwable.getMessage() != null) {
            cause += ": " + throwable.getMessage();
        }
        String longMessage = message + ": " + cause;
        // This method will only be called in exceptional situations, so the cost of looking up the logger won't be bad:
        Logger logger = LogManager.getLogger(getClass());
        if (logger.isTraceEnabled()) {
            logger.trace(message, throwable);
        } else {
            logger.warn(longMessage);
        }
        // We don't want to wrap the Throwable here because it is probably not one of the exceptions that ElasticsearchException can
        // serialize:
        return new ElasticsearchException(longMessage);
    }
}
