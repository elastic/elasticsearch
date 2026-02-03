/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import java.util.Map;

/**
 * Supplies additional data for the logging.
 * The data comes as a map of fields, which may be different each call depending on the state of the system.
 */
public abstract class ActionLoggingFields {

    protected final ActionLoggingFieldsContext context;

    public ActionLoggingFields(ActionLoggingFieldsContext context) {
        this.context = context;
    }

    /**
     * Produce the logging fields, depending on the state of the system.
     * @return map of field name to value
     */
    public Map<String, String> logFields() {
        return Map.of();
    }
}
