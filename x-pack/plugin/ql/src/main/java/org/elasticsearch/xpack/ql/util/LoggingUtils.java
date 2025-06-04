/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

public final class LoggingUtils {

    private LoggingUtils() {}

    public static void logOnFailure(Logger logger, Throwable throwable) {
        RestStatus status = ExceptionsHelper.status(throwable);
        logger.log(status.getStatus() >= 500 ? Level.WARN : Level.DEBUG, () -> "Request failed with status [" + status + "]: ", throwable);
    }

}
