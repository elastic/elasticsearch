/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.common.logging.DynamicContextDataProvider;

import java.util.Map;

/**
 * Elasticsearch plugins may provide an implementation of this class (via SPI) in order to add extra fields to the JSON based log file.
 *
 * @see DynamicContextDataProvider
 */
public interface LoggingDataProvider {
    void collectData(Map<String, String> data);
}
