/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xcontent;

import org.elasticsearch.core.RestApiVersion;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Holds a field that can be found in a request while parsing and its different
 * variants, which may be deprecated.
 */
public class ParseField {
    private final String name;
    private final String[] deprecatedNames;
    private final Function<RestApiVersion, Boolean> forRestApiVersion;
    private final String allReplacedWith;
    private final boolean fullyDeprecated;

    private final String[] allNames;

    private static final String[] EMPTY = new String[0];

    private ParseField(
        String name,
        Function<RestApiVersion, Boolean> forRestApiVersion,
        String[] deprecatedNames,
        boolean fullyDeprecated,
        String allReplacedWith
    ) {
        this.name = name;
        this.fullyDeprecated = fullyDeprecated;
        this.allReplacedWith = allReplacedWith;
        if (deprecatedNames == null || deprecatedNames.length == 0) {
            this.deprecatedNames = EMPTY;
        } else {
            final HashSet<String> set = new HashSet<>();
            Collections.addAll(set, deprecatedNames);
            this.deprecatedNames = set.toArray(new String[set.size()]);
        }
        this.forRestApiVersion = forRestApiVersion;

        Set<String> names = new HashSet<>();
        names.add(name);
        Collections.addAll(names, this.deprecatedNames);
        this.allNames = names.toArray(new String[names.size()]);
    }

    /**
     * Creates a field available for lookup for both current and previous REST API versions
     * @param name            the primary name for this field. This will be returned by
     *                        {@link #getPreferredName()}
     * @param deprecatedNames names for this field which are deprecated and will not be
     *                        accepted when strict matching is used.
     */
    public ParseField(String name, String... deprecatedNames) {
        this(name, RestApiVersion.onOrAfter(RestApiVersion.minimumSupported()), deprecatedNames, false, null);
    }

    /**
     * @return the preferred name used for this field
     */
    public String getPreferredName() {
        return name;
    }

    /**
     * @return All names for this field regardless of whether they are
     *         deprecated
     */
    public String[] getAllNamesIncludedDeprecated() {
        return allNames;
    }

    /**
     * @param deprecatedNamesOverride
     *            deprecated names to include with the returned
     *            {@link ParseField}
     * @return a new {@link ParseField} using the preferred name from this one
     *         but with the specified deprecated names
     */
    public ParseField withDeprecation(String... deprecatedNamesOverride) {
        return new ParseField(this.name, this.forRestApiVersion, deprecatedNamesOverride, this.fullyDeprecated, this.allReplacedWith);
    }

    /**
     * Creates a new field with current name and deprecatedNames, but overrides forRestApiVersion
     * @param forRestApiVersionOverride - a boolean function indicating for what version a deprecated name is available
     */
    public ParseField forRestApiVersion(Function<RestApiVersion, Boolean> forRestApiVersionOverride) {
        return new ParseField(this.name, forRestApiVersionOverride, this.deprecatedNames, this.fullyDeprecated, this.allReplacedWith);
    }

    /**
     * @return a function indicating for which RestApiVersion a deprecated name is declared for
     */
    public Function<RestApiVersion, Boolean> getForRestApiVersion() {
        return forRestApiVersion;
    }

    /**
     * Return a new ParseField where all field names are deprecated and replaced
     * with {@code allReplacedWith}.
     */
    public ParseField withAllDeprecated(String allReplacedWithOverride) {
        return new ParseField(
            this.name,
            this.forRestApiVersion,
            getAllNamesIncludedDeprecated(),
            this.fullyDeprecated,
            allReplacedWithOverride
        );
    }

    /**
     * Return a new ParseField where all field names are deprecated with no replacement
     */
    public ParseField withAllDeprecated() {
        return new ParseField(this.name, this.forRestApiVersion, getAllNamesIncludedDeprecated(), true, this.allReplacedWith);
    }

    /**
     * Does {@code fieldName} match this field?
     * @param fieldName
     *            the field name to match against this {@link ParseField}
     * @param deprecationHandler called if {@code fieldName} is deprecated
     * @return true if <code>fieldName</code> matches any of the acceptable
     *         names for this {@link ParseField}.
     */
    public boolean match(String fieldName, DeprecationHandler deprecationHandler) {
        return match(null, () -> XContentLocation.UNKNOWN, fieldName, deprecationHandler);
    }

    /**
     * Does {@code fieldName} match this field?
     * @param parserName
     *            the name of the parent object holding this field
     * @param location
     *            the XContentLocation of the field
     * @param fieldName
     *            the field name to match against this {@link ParseField}
     * @param deprecationHandler called if {@code fieldName} is deprecated
     * @return true if <code>fieldName</code> matches any of the acceptable
     *         names for this {@link ParseField}.
     */
    public boolean match(String parserName, Supplier<XContentLocation> location, String fieldName, DeprecationHandler deprecationHandler) {
        Objects.requireNonNull(fieldName, "fieldName cannot be null");
        // if this parse field has not been completely deprecated then try to
        // match the preferred name
        if (fullyDeprecated == false && allReplacedWith == null && fieldName.equals(name)) {
            return true;
        }
        boolean isCompatibleDeprecation = RestApiVersion.minimumSupported().matches(forRestApiVersion)
            && RestApiVersion.current().matches(forRestApiVersion) == false;

        // Now try to match against one of the deprecated names. Note that if
        // the parse field is entirely deprecated (allReplacedWith != null) all
        // fields will be in the deprecatedNames array
        for (String depName : deprecatedNames) {
            if (fieldName.equals(depName)) {
                if (fullyDeprecated) {
                    deprecationHandler.logRemovedField(parserName, location, fieldName, isCompatibleDeprecation);
                } else if (allReplacedWith == null) {
                    deprecationHandler.logRenamedField(parserName, location, fieldName, name, isCompatibleDeprecation);
                } else {
                    deprecationHandler.logReplacedField(parserName, location, fieldName, allReplacedWith, isCompatibleDeprecation);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return getPreferredName();
    }

    /**
     * @return the message to use if this {@link ParseField} has been entirely
     *         deprecated in favor of something else. This method will return
     *         <code>null</code> if the ParseField has not been completely
     *         deprecated.
     */
    public String getAllReplacedWith() {
        return allReplacedWith;
    }

    /**
     * @return an array of the names for the {@link ParseField} which are
     *         deprecated.
     */
    public String[] getDeprecatedNames() {
        return deprecatedNames;
    }

    public static class CommonFields {
        public static final ParseField FIELD = new ParseField("field");
        public static final ParseField FIELDS = new ParseField("fields");
        public static final ParseField FORMAT = new ParseField("format");
        public static final ParseField MISSING = new ParseField("missing");
        public static final ParseField TIME_ZONE = new ParseField("time_zone");

        protected CommonFields() {}
    }
}
