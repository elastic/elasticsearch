/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.logging.log4j.Level;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * {@code DeprecationRestHandler} provides a proxy for any existing {@link RestHandler} so that usage of the handler can be
 * logged using the {@link DeprecationLogger}.
 */
public class DeprecationRestHandler extends FilterRestHandler implements RestHandler {

    public static final String DEPRECATED_ROUTE_KEY = "deprecated_route";

    private final String deprecationMessage;
    private final DeprecationLogger deprecationLogger;
    private final String deprecationKey;
    @Nullable
    private final Level deprecationLevel;

    /**
     * Create a {@link DeprecationRestHandler} that encapsulates the {@code handler} using the {@code deprecationLogger} to log
     * deprecation {@code warning}.
     *
     * @param handler The rest handler to deprecate (it's possible that the handler is reused with a different name!)
     * @param method a method of a deprecated endpoint
     * @param path a path of a deprecated endpoint
     * @param deprecationMessage The message to warn users with when they use the {@code handler}
     * @param deprecationLogger The deprecation logger
     * @throws NullPointerException if any parameter except {@code deprecationMessage} is {@code null}
     * @throws IllegalArgumentException if {@code deprecationMessage} is not a valid header
     */
    public DeprecationRestHandler(
        RestHandler handler,
        RestRequest.Method method,
        String path,
        @Nullable Level deprecationLevel,
        String deprecationMessage,
        DeprecationLogger deprecationLogger
    ) {
        super(handler);
        this.deprecationMessage = requireValidHeader(deprecationMessage);
        this.deprecationLogger = Objects.requireNonNull(deprecationLogger);
        this.deprecationKey = DEPRECATED_ROUTE_KEY + "_" + method + "_" + path;
        if (deprecationLevel != null && (deprecationLevel != Level.WARN && deprecationLevel != DeprecationLogger.CRITICAL)) {
            throw new IllegalArgumentException(
                "unexpected deprecation logger level: " + deprecationLevel + ", expected either 'CRITICAL' or 'WARN'"
            );
        }
        this.deprecationLevel = deprecationLevel;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Usage is logged via the {@link DeprecationLogger} so that the actual response can be notified of deprecation as well.
     */
    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        // The default value for deprecated requests without a version warning is CRITICAL
        if (deprecationLevel == null || deprecationLevel == DeprecationLogger.CRITICAL) {
            deprecationLogger.critical(DeprecationCategory.API, deprecationKey, deprecationMessage);
        } else {
            deprecationLogger.warn(DeprecationCategory.API, deprecationKey, deprecationMessage);
        }

        getDelegate().handleRequest(request, channel, client);
    }

    /**
     * This does a very basic pass at validating that a header's value contains only expected characters according to RFC-5987, and those
     * that it references.
     * <p>
     * https://tools.ietf.org/html/rfc5987
     * <p>
     * This is only expected to be used for assertions. The idea is that only readable US-ASCII characters are expected; the rest must be
     * encoded with percent encoding, which makes checking for a valid character range very simple.
     *
     * @param value The header value to check
     * @return {@code true} if the {@code value} is not obviously wrong.
     */
    public static boolean validHeaderValue(String value) {
        if (Strings.hasText(value) == false) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            // 32 = ' ' (31 = unit separator); 126 = '~' (127 = DEL)
            if (c < 32 || c > 126) {
                return false;
            }
        }

        return true;
    }

    /**
     * Throw an exception if the {@code value} is not a {@link #validHeaderValue(String) valid header}.
     *
     * @param value The header value to check
     * @return Always {@code value}.
     * @throws IllegalArgumentException if {@code value} is not a {@link #validHeaderValue(String) valid header}.
     */
    public static String requireValidHeader(String value) {
        if (validHeaderValue(value) == false) {
            throw new IllegalArgumentException("header value must contain only US ASCII text");
        }

        return value;
    }
}
