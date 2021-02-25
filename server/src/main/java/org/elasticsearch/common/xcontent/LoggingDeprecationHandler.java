/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.util.function.BiConsumer;
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
     *
     * This uses ParseField's logger because that is the logger that
     * we have been using for many releases for deprecated fields.
     * Changing that will require some research to make super duper
     * sure it is safe.
     */
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ParseField.class);

    public static final String COMPATIBLE_API_WARNING_PREFIX = "";
    public static final LoggingDeprecationHandler INSTANCE = new LoggingDeprecationHandler(( message, params) ->
        deprecationLogger.deprecate(DeprecationCategory.API,"deprecated_field", message, params));


    public static final LoggingDeprecationHandler COMPATIBLE_REST_API_INSTANCE = new LoggingDeprecationHandler((message, params) ->
        deprecationLogger.compatibleApiWarning("deprecated_field",COMPATIBLE_API_WARNING_PREFIX+ message, params));


    private BiConsumer<String, Object[]> loggingFunction;

    private LoggingDeprecationHandler() {
        // Singleton
    }
    private LoggingDeprecationHandler(BiConsumer<String,Object[]> loggingFunction) {
        // Singleton
        this.loggingFunction = loggingFunction;
    }
    @Override
    public void usedDeprecatedName(String parserName, Supplier<XContentLocation> location, String usedName, String modernName) {
        String prefix = parserLocation(parserName, location);
        loggingFunction.accept("{}Deprecated field [{}] used, expected [{}] instead", new Object[]{prefix, usedName, modernName});
    }

    @Override
    public void usedDeprecatedField(String parserName, Supplier<XContentLocation> location, String usedName, String replacedWith) {
        String prefix = parserLocation(parserName, location);
        loggingFunction.accept("{}Deprecated field [{}] used, replaced by [{}]", new Object[] {prefix, usedName, replacedWith});
    }

    @Override
    public void usedDeprecatedField(String parserName, Supplier<XContentLocation> location, String usedName) {
        String prefix = parserLocation(parserName, location);
        loggingFunction.accept("{}Deprecated field [{}] used, this field is unused and will be removed entirely",
            new Object[]{prefix, usedName});
    }

    private String parserLocation(String parserName, Supplier<XContentLocation> location) {
        return parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
    }
    @Override
    public DeprecationHandler getInstance(boolean compatibleWarnings) {
        if (compatibleWarnings) {
            return COMPATIBLE_REST_API_INSTANCE;
        }
        return INSTANCE;
    }
}
