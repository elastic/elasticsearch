/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

/**
 * A TaskListener that just logs the response at the info level. Used when we
 * need a listener but aren't returning the result to the user.
 */
public final class LoggingTaskListener<Response> implements TaskListener<Response> {
    private static final Logger logger = LogManager.getLogger(LoggingTaskListener.class);

    /**
     * Get the instance of NoopActionListener cast appropriately.
     */
    @SuppressWarnings("unchecked") // Safe because we only toString the response
    public static <Response> TaskListener<Response> instance() {
        return (TaskListener<Response>) INSTANCE;
    }

    private static final LoggingTaskListener<Object> INSTANCE = new LoggingTaskListener<>();

    private LoggingTaskListener() {}

    @Override
    public void onResponse(Task task, Response response) {
        logger.info("{} finished with response {}", task.getId(), response);
    }

    @Override
    public void onFailure(Task task, Exception e) {
        logger.warn(() -> new ParameterizedMessage("{} failed with exception", task.getId()), e);
    }
}
