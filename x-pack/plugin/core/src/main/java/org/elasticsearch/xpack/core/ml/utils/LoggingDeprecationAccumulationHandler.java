/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;

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

    private final List<String> deprecations = new ArrayList<>();

    @Override
    public void usedDeprecatedName(String usedName, String modernName) {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedName(usedName, modernName);
        deprecations.add(LoggerMessageFormat.format("Deprecated field [{}] used, expected [{}] instead",
            new Object[] {usedName, modernName}));
    }

    @Override
    public void usedDeprecatedField(String usedName, String replacedWith) {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedField(usedName, replacedWith);
        deprecations.add(LoggerMessageFormat.format("Deprecated field [{}] used, replaced by [{}]",
            new Object[] {usedName, replacedWith}));
    }

    /**
     * The collected deprecation warnings
     */
    public List<String> getDeprecations() {
        return Collections.unmodifiableList(deprecations);
    }

}
