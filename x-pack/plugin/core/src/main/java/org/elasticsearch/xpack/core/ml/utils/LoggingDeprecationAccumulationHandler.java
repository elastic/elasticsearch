/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.DeprecationHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Very similar to {@link org.elasticsearch.common.xcontent.LoggingDeprecationHandler} main differences are:
 * 1. Is not a Singleton
 * 2. Accumulates all deprecation warnings into a list that can be retrieved
 *    with {@link LoggingDeprecationAccumulationHandler#getDeprecations()}
 *
 * NOTE: The accumulation is NOT THREAD SAFE
 */
public class LoggingDeprecationAccumulationHandler implements DeprecationHandler {
    /**
     * The logger to which to send deprecation messages.
     */
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(ParseField.class));

    private final List<String> deprecations = new ArrayList<>();

    @Override
    public void usedDeprecatedName(String usedName, String modernName) {
        String formattedMessage = LoggerMessageFormat.format("Deprecated field [{}] used, expected [{}] instead",
            usedName,
            modernName);
        deprecationLogger.deprecated(formattedMessage);
        deprecations.add(formattedMessage);
    }

    @Override
    public void usedDeprecatedField(String usedName, String replacedWith) {
        String formattedMessage = LoggerMessageFormat.format("Deprecated field [{}] used, replaced by [{}]",
            usedName,
            replacedWith);
        deprecationLogger.deprecated(formattedMessage);
        deprecations.add(formattedMessage);
    }

    /**
     * The collected deprecation warnings
     */
    public List<String> getDeprecations() {
        return Collections.unmodifiableList(deprecations);
    }

}
