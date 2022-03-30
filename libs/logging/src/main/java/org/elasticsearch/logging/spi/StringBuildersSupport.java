/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

import org.elasticsearch.logging.locator.LoggingSupportLocator;


public interface StringBuildersSupport {
    StringBuildersSupport STRING_BUILDERS_SUPPORT = LoggingSupportLocator.STRING_BUILDERS_SUPPORT_INSTANCE;

    static void escapeJson(final StringBuilder toAppendTo, final int start) {
        STRING_BUILDERS_SUPPORT.escapeJsonImpl(toAppendTo, start);
    }

    void escapeJsonImpl(final StringBuilder toAppendTo, final int start);
}

