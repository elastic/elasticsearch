/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.logging.Level;

/**
 * Generic writer interface to record a log message.
 */
public interface ActivityLogWriter {
    ActivityLogWriter NOOP = (l, m) -> {};

    void write(Level level, ESLogMessage message);
}
