/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.bootstrap;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogMessageUtil {
    private LogMessageUtil() {}

    public static String asJsonArray(Stream<String> stream) {
        return "[" + stream.map(LogMessageUtil::inQuotes).collect(Collectors.joining(", ")) + "]";
    }

    public static String inQuotes(String s) {
        if (s == null) return inQuotes("");
        return "\"" + s + "\"";
    }
}
