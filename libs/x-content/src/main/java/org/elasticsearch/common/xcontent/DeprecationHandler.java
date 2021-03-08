/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import java.util.function.Supplier;

/**
 * Callback for notifying the creator of the {@link XContentParser} that
 * parsing hit a deprecated field.
 */
public interface DeprecationHandler {
    /**
     * Throws an {@link UnsupportedOperationException} when parsing hits a
     * deprecated field. Use this when creating an {@link XContentParser}
     * that won't interact with deprecation logic at all or when you want
     * to fail fast when parsing deprecated fields.
     */
    DeprecationHandler THROW_UNSUPPORTED_OPERATION = new DeprecationHandler() {
        @Override
        public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {
            if (parserName != null) {
                throw new UnsupportedOperationException("deprecated fields not supported in [" + parserName + "] but got ["
                    + oldName + "] at [" + location.get() + "] which is a deprecated name for [" + replacedName + "]");
            } else {
                throw new UnsupportedOperationException("deprecated fields not supported here but got ["
                    + oldName + "] which is a deprecated name for [" + replacedName + "]");
            }
        }
        @Override
        public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {
            if (parserName != null) {
                throw new UnsupportedOperationException("deprecated fields not supported in [" + parserName + "] but got ["
                    + oldName + "] at [" + location.get() + "] which has been replaced with [" + currentName + "]");
            } else {
                throw new UnsupportedOperationException("deprecated fields not supported here but got ["
                    + oldName + "] which has been replaced with [" + currentName + "]");
            }
        }

        @Override
        public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {
            if (parserName != null) {
                throw new UnsupportedOperationException("deprecated fields not supported in [" + parserName + "] but got ["
                    + removedName + "] at [" + location.get() + "] which has been deprecated entirely");
            } else {
                throw new UnsupportedOperationException("deprecated fields not supported here but got ["
                    + removedName + "] which has been deprecated entirely");
            }
        }
    };

    /**
     * Ignores all deprecations
     */
    DeprecationHandler IGNORE_DEPRECATIONS = new DeprecationHandler() {
        @Override
        public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {

        }

        @Override
        public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {

        }

        @Override
        public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {

        }
    };

    /**
     * Called when the provided field name matches a deprecated name for the field.
     * @param oldName the provided field name
     * @param currentName the modern name for the field
     */
    void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName);

    /**
     * Called when the provided field name matches the current field but the entire
     * field has been marked as deprecated and another field should be used
     * @param oldName the provided field name
     * @param replacedName the name of the field that replaced this field
     */
    void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName);

    /**
     * Called when the provided field name matches the current field but the entire
     * field has been marked as deprecated with no replacement
     * Emits a compatible api warning instead of deprecation warning when isCompatibleDeprecation is true
     * @param removedName the provided field name
     */
    void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName);

    /**
     * @see DeprecationHandler#logRenamedField(String, Supplier, String, String)
     * Emits a compatible api warning instead of deprecation warning when isCompatibleDeprecation is true
     */
    default void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName,
                                 boolean isCompatibleDeprecation) {
        logRenamedField(parserName, location, oldName, currentName);
    }

    /**
     * @see DeprecationHandler#logReplacedField(String, Supplier, String, String)
     * Emits a compatible api warning instead of deprecation warning when isCompatibleDeprecation is true
     */
    default void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName,
                                  boolean isCompatibleDeprecation) {
        logReplacedField(parserName, location, oldName, replacedName);
    }

    /**
     * @see DeprecationHandler#logRemovedField(String, Supplier, String)
     * Emits a compatible api warning instead of deprecation warning when isCompatibleDeprecation is true
     */
    default void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName,
                                 boolean isCompatibleDeprecation) {
        logRemovedField(parserName, location, removedName);
    }

}
