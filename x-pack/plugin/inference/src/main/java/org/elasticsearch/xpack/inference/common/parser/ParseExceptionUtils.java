/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentParseException;

public final class ParseExceptionUtils {

    private static final Logger logger = LogManager.getLogger(ParseExceptionUtils.class);

    private ParseExceptionUtils() {}

    public static RuntimeException unwrapXContentParseException(Throwable t) {
        var unwrapped = doUnwrap(t);
        if (unwrapped instanceof RuntimeException runtimeException) {
            return runtimeException;
        } else {
            return new RuntimeException(unwrapped);
        }
    }

    private static Throwable doUnwrap(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof XContentParseException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter++ > 10) {
                logger.warn("XContentParseException cause unwrapping ran for 10 levels, skipping", t);
                return result;
            }
            result = result.getCause();
        }
        return result;
    }
}
