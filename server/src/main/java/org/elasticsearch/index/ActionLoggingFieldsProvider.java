/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

/**
 * Interface for providing additional fields for the logging from a plugin.
 * Intended to be loaded through SPI.
 * This provider generates {@link ActionLoggingFields} instances that depend on current configuration contents.
 * The {@link ActionLoggingFields} produce actual field maps.
 * <p>
 * This API is intended to be used with slow logs, query logs, etc. and the fields are produced as data and should
 * be added to the specific log explicitly by the logger.
 */
public interface ActionLoggingFieldsProvider {
    /**
     * Create a field provider.
     */
    ActionLoggingFields create(ActionLoggingFieldsContext context);
}
