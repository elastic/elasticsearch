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
import org.elasticsearch.action.ActionListener;

import static org.elasticsearch.core.Strings.format;

/**
 * An {@link ActionListener} that just logs the task and its response at the info level. Used when we need a listener but aren't returning
 * the result to the user.
 */
public final class LoggingTaskListener<Response> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(LoggingTaskListener.class);

    private final Task task;

    public LoggingTaskListener(Task task) {
        this.task = task;
    }

    @Override
    public void onResponse(Response response) {
        logger.info("{} finished with response {}", task.getId(), response);

    }

    @Override
    public void onFailure(Exception e) {
        logger.warn(() -> format("%s failed with exception", task.getId()), e);
    }
}
