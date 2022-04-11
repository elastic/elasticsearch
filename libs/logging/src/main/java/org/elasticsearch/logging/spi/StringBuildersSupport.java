/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

//TODO PG remove.. .
public interface StringBuildersSupport {

    static StringBuildersSupport provider() {
        return LoggingSupportProvider.provider().stringBuildersSupport();

    }

    static void escapeJson(StringBuilder toAppendTo, int start) {
        provider().escapeJsonImpl(toAppendTo, start);
    }

    void escapeJsonImpl(StringBuilder toAppendTo, int start);
}
