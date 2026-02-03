/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logging;

import java.util.Map;

/**
 * Generic log message interface. Supports logging messages which are a collection of fields.
 */
public interface LogMessage {
    LogMessage with(String key, Object value);

    LogMessage withFields(Map<String, Object> fields);

    Object get(String key);
}
