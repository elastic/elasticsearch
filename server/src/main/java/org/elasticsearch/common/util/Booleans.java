/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.SuppressForbidden;

public class Booleans {

    private static final String DEPRECATED_MESSAGE_TEMPLATE = "[{}] method is being used in a lenient way to read value: {}. "
        + "This is discouraged in Elasticsearch as it treats all values different than 'true' as false. "
        + "The strict method from Booleans util class should be used instead.";

    static class DeprecationLoggerHolder {
        static DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Booleans.class);
    }

    @SuppressForbidden(reason = "wrap lenient parsing of booleans for deprecation logging.")
    public static boolean parseBoolean(String value) {
        if ("true".equals(value) == false && "false".equals(value) == false) {
            String key = "Boolean#parseBoolean";
            Booleans.DeprecationLoggerHolder.deprecationLogger.warn(
                DeprecationCategory.PARSING,
                key,
                DEPRECATED_MESSAGE_TEMPLATE,
                key,
                value
            );
        }
        return Boolean.parseBoolean(value);
    }
}
