/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentLocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

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
    public void usedDeprecatedName(String parserName, Supplier<XContentLocation> location, String usedName, String modernName) {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedName(parserName, location, usedName, modernName);
        String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
        deprecations.add(LoggerMessageFormat.format("{}Deprecated field [{}] used, expected [{}] instead",
            new Object[]{prefix, usedName, modernName}));
    }

    @Override
    public void usedDeprecatedField(String parserName, Supplier<XContentLocation> location, String usedName, String replacedWith) {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedField(parserName, location, usedName, replacedWith);
        String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
        deprecations.add(LoggerMessageFormat.format("{}Deprecated field [{}] used, replaced by [{}]",
            new Object[]{prefix, usedName, replacedWith}));
    }

    @Override
    public void usedDeprecatedField(String parserName, Supplier<XContentLocation> location, String usedName) {
        LoggingDeprecationHandler.INSTANCE.usedDeprecatedField(parserName, location, usedName);
        String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
        deprecations.add(LoggerMessageFormat.format("{}Deprecated field [{}] used, unused and will be removed entirely",
            new Object[]{prefix, usedName}));
    }

    /**
     * The collected deprecation warnings
     */
    public List<String> getDeprecations() {
        return Collections.unmodifiableList(deprecations);
    }

}
