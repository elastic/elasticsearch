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
 * The underlying object is assumed to be mutable and `with` methods mutate it and return
 * `this` to facilitate more fluent code.
 */
public interface LogMessage {
    /**
     * Add a field to the log message.
     */
    LogMessage with(String key, Object value);

    /**
     * Add a set of fields to the log message.
     */
    LogMessage withFields(Map<String, Object> fields);

    /**
     * Fetch a field value. Will return `null` if the field does not exist. Mostly useful for testing.
     */
    Object get(String key);
}
