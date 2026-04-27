/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.tasks.Task;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public record LoggingReindexTaskListener(Task task) implements ActionListener<BulkByScrollResponse> {

    private static final Logger logger = LogManager.getLogger(LoggingReindexTaskListener.class);

    public LoggingReindexTaskListener {
        Objects.requireNonNull(task);
    }

    @Override
    public void onResponse(BulkByScrollResponse resp) {
        logger.info("{} finished with response {}", task.getId(), resp);
    }

    @Override
    public void onFailure(Exception e) {
        if (e instanceof TaskRelocatedException relocatedException) {
            logger.info("{} was relocated to {}", task.getId(), relocatedException.getRelocatedTaskId().orElseThrow());
        } else {
            logger.warn(() -> format("%s failed with exception", task.getId()), e);
        }
    }
}
