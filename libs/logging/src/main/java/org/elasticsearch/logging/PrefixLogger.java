/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.spi.LogManagerFactory;

import java.util.Arrays;
import java.util.stream.Stream;

public class PrefixLogger {

    public PrefixLogger() {}

    private static final String SPACE = " ";

    public static org.elasticsearch.logging.Logger getLogger(Class<?> clazz, int shardId, String... prefixes) {
        return getLogger(clazz, Stream.concat(Stream.of(Integer.toString(shardId)), Arrays.stream(prefixes)).toArray(String[]::new));
    }

    // /**
    // * Just like {@link #getLogger(Class, ShardId, String...)} but String loggerName instead of
    // * Class and no extra prefixes. // TODO: fix docs
    // */
    public static org.elasticsearch.logging.Logger getLogger(String loggerName, String indexName, int shardId) {
        String prefix = formatPrefix(indexName, Integer.toString(shardId));
        return LogManagerFactory.provider().getPrefixLogger(loggerName, prefix);

    }

    public static org.elasticsearch.logging.Logger getLoggerWithIndexName(Class<?> clazz, String indexName, String... prefixes) {
        return getLogger(clazz, Stream.concat(Stream.of(SPACE, indexName), Arrays.stream(prefixes)).toArray(String[]::new));
    }

    public static org.elasticsearch.logging.Logger getLogger(Class<?> clazz, String... prefixes) {
        return LogManagerFactory.provider().getPrefixLogger(clazz, formatPrefix(prefixes));

    }

    public static org.elasticsearch.logging.Logger getLogger(org.elasticsearch.logging.Logger parentLogger, String s) {
        // TODO PG finish this.. if possible
        org.elasticsearch.logging.Logger inner = org.elasticsearch.logging.LogManager.getLogger(parentLogger.getName() + s);
        // if (parentLogger instanceof org.elasticsearch.logging.impl.PrefixLogger) {
        // return new LoggerImpl(
        // new org.elasticsearch.logging.impl.PrefixLogger(
        // Util.log4jLogger(inner),
        // ((org.elasticsearch.logging.impl.PrefixLogger) parentLogger).prefix()
        // )
        // );
        // }
        return parentLogger;
    }

    private static String formatPrefix(String... prefixes) {
        String prefix = null;
        if (prefixes != null && prefixes.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (String prefixX : prefixes) {
                if (prefixX != null) {
                    if (prefixX.equals(SPACE)) {
                        sb.append(" ");
                    } else {
                        sb.append("[").append(prefixX).append("]");
                    }
                }
            }
            if (sb.length() > 0) {
                prefix = sb.toString();
            }
        }
        return prefix;
    }

}
