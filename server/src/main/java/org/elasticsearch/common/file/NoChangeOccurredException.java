/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.file;

import java.util.concurrent.ExecutionException;

/**
 * Indicates that {@link AbstractFileWatchingService#processFileChanges()} had no effect,
 * and listeners should not be notified because there's nothing for them to do.
 */
public class NoChangeOccurredException extends ExecutionException {
    public NoChangeOccurredException(Throwable cause) {
        super(cause);
    }
}
