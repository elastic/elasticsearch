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
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.UpdateForV10;

import java.util.Locale;

import static org.elasticsearch.common.util.LenientBooleans.DeprecationLoggerHolder.deprecationLogger;

@UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA)
public class LenientBooleans {

    /**
     * Category of use of lenient Boolean parsing.
     */
    public enum Category {
        SYSTEM_PROPERTY("system property"),
        INDEX_METADATA("index metadata");

        private final String displayValue;

        Category(String displayValue) {
            this.displayValue = displayValue;
        }

        public String displayValue() {
            return displayValue;
        }
    }

    private static final String DEPRECATED_MESSAGE_TEMPLATE = "Usage of lenient boolean value [{}] for {} [{}] was deprecated. "
        + "Future releases of Elasticsearch may only accept `true` or `false`.";

    static class DeprecationLoggerHolder {
        static DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(LenientBooleans.class);
    }

    @SuppressForbidden(reason = "wrap lenient parsing of booleans for deprecation logging.")
    public static boolean parseAndCheckForDeprecatedUsage(String value, Category category, String name) {
        if (Booleans.isBoolean(value) == false) {
            final String method = "Boolean#parseBoolean";
            String key = String.format(Locale.ROOT, "%s.%s", method, category);
            deprecationLogger.warn(DeprecationCategory.PARSING, key, DEPRECATED_MESSAGE_TEMPLATE, value, category.displayValue(), name);
        }
        return Boolean.parseBoolean(value);
    }
}
