/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentLocation;

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
    public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {
        LoggingDeprecationHandler.INSTANCE.logRenamedField(parserName, location, oldName, currentName);
        String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
        deprecations.add(
            LoggerMessageFormat.format("{}Deprecated field [{}] used, expected [{}] instead", new Object[] { prefix, oldName, currentName })
        );
    }

    @Override
    public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {
        LoggingDeprecationHandler.INSTANCE.logReplacedField(parserName, location, oldName, replacedName);
        String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
        deprecations.add(
            LoggerMessageFormat.format("{}Deprecated field [{}] used, replaced by [{}]", new Object[] { prefix, oldName, replacedName })
        );
    }

    @Override
    public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {
        LoggingDeprecationHandler.INSTANCE.logRemovedField(parserName, location, removedName);
        String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
        deprecations.add(
            LoggerMessageFormat.format(
                "{}Deprecated field [{}] used, unused and will be removed entirely",
                new Object[] { prefix, removedName }
            )
        );
    }

    /**
     * The collected deprecation warnings
     */
    public List<String> getDeprecations() {
        return Collections.unmodifiableList(deprecations);
    }

}
