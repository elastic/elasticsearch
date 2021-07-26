/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.util.function.Supplier;

/**
 * Logs deprecations to the {@link DeprecationLogger}.
 * <p>
 * This is core's primary implementation of {@link DeprecationHandler} and
 * should <strong>absolutely</strong> be used everywhere where it parses
 * requests. It is much less appropriate when parsing responses from external
 * sources because it will report deprecated fields back to the user as
 * though the user sent them.
 */
public class LoggingDeprecationHandler implements DeprecationHandler {
    /**
     * The logger to which to send deprecation messages.
     * <p>
     * This uses ParseField's logger because that is the logger that
     * we have been using for many releases for deprecated fields.
     * Changing that will require some research to make super duper
     * sure it is safe.
     */
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ParseField.class);

    public static final LoggingDeprecationHandler INSTANCE = new LoggingDeprecationHandler();

    private TriConsumer<String, Object[], String> deprecationLoggerFunction = (message, params, field_name) ->
        deprecationLogger.deprecate(DeprecationCategory.API, "deprecated_field_" + field_name, message, params);

    private TriConsumer<String, Object[], String> compatibleLoggerFunction = (message, params, field_name) ->
        deprecationLogger.compatibleApiWarning("deprecated_field_" + field_name, message, params);

    private LoggingDeprecationHandler() {
        // one instance only
    }

    @Override
    public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {
        logRenamedField(parserName, location, oldName, currentName, false);
    }

    @Override
    public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {
        logReplacedField(parserName, location, oldName, replacedName, false);
    }

    @Override
    public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {
        logRemovedField(parserName, location, removedName, false);
    }

    @Override
    public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName,
                                boolean isCompatibleDeprecation) {
        String prefix = parserLocation(parserName, location);
        TriConsumer<String, Object[], String> loggingFunction = getLoggingFunction(isCompatibleDeprecation);
        loggingFunction.apply("{}Deprecated field [{}] used, expected [{}] instead",
            new Object[]{prefix, oldName, currentName}, oldName);
    }

    @Override
    public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName,
                                 boolean isCompatibleDeprecation) {
        String prefix = parserLocation(parserName, location);
        TriConsumer<String, Object[], String> loggingFunction = getLoggingFunction(isCompatibleDeprecation);
        loggingFunction.apply("{}Deprecated field [{}] used, replaced by [{}]",
            new Object[]{prefix, oldName, replacedName}, oldName);
    }

    @Override
    public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName,
                                boolean isCompatibleDeprecation) {
        String prefix = parserLocation(parserName, location);
        TriConsumer<String, Object[], String> loggingFunction = getLoggingFunction(isCompatibleDeprecation);
        loggingFunction.apply("{}Deprecated field [{}] used, this field is unused and will be removed entirely",
            new Object[]{prefix, removedName}, removedName);

    }

    private String parserLocation(String parserName, Supplier<XContentLocation> location) {
        return parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
    }

    private TriConsumer<String, Object[], String> getLoggingFunction(boolean isCompatibleDeprecation) {
        if (isCompatibleDeprecation) {
            return compatibleLoggerFunction;
        } else {
            return deprecationLoggerFunction;
        }
    }
}
