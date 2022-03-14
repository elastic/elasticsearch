/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.api.core;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Message;

public interface LogEvent {
    Level getLevel();

    String getLoggerName();

    Throwable getThrown();

    Message getMessage();
}
