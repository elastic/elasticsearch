/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry for looking things up using ParseField semantics.
 */
public class ParseFieldRegistry<T> {
    private final Map<String, Tuple<ParseField, T>> registry = new HashMap<>();
    private final String registryName;

    /**
     * Build the registry.
     * @param registryName used for error messages
     */
    public ParseFieldRegistry(String registryName) {
        this.registryName = registryName;
    }

    /**
     * All the names under which values are registered. Expect this to be used mostly for testing.
     */
    public Set<String> getNames() {
        return registry.keySet();
    }

    /**
     * Register a parser.
     */
    public void register(T value, String name) {
        register(value, new ParseField(name));
    }

    /**
     * Register a parser.
     */
    public void register(T value, ParseField parseField) {
        Tuple<ParseField, T> parseFieldParserTuple = new Tuple<>(parseField, value);
        for (String name : parseField.getAllNamesIncludedDeprecated()) {
            Tuple<ParseField, T> previousValue = registry.putIfAbsent(name, parseFieldParserTuple);
            if (previousValue != null) {
                throw new IllegalArgumentException(
                    "["
                        + previousValue.v2()
                        + "] already registered for ["
                        + registryName
                        + "]["
                        + name
                        + "] while trying to register ["
                        + value
                        + "]"
                );
            }
        }
    }

    /**
     * Lookup a value from the registry by name while checking that the name matches the ParseField.
     *
     * @param name The name of the thing to look up.
     * @return The value being looked up. Never null.
     * @throws ParsingException if the named thing isn't in the registry or the name was deprecated and deprecated names aren't supported.
     */
    public T lookup(String name, XContentLocation xContentLocation, DeprecationHandler deprecationHandler) {
        T value = lookupReturningNullIfNotFound(name, deprecationHandler);
        if (value == null) {
            throw new ParsingException(xContentLocation, "no [" + registryName + "] registered for [" + name + "]");
        }
        return value;
    }

    /**
     * Lookup a value from the registry by name while checking that the name matches the ParseField.
     *
     * @param name The name of the thing to look up.
     * @return The value being looked up or null if it wasn't found.
     * @throws ParsingException if the named thing isn't in the registry or the name was deprecated and deprecated names aren't supported.
     */
    public T lookupReturningNullIfNotFound(String name, DeprecationHandler deprecationHandler) {
        Tuple<ParseField, T> parseFieldAndValue = registry.get(name);
        if (parseFieldAndValue == null) {
            return null;
        }
        ParseField parseField = parseFieldAndValue.v1();
        T value = parseFieldAndValue.v2();
        boolean match = parseField.match(name, deprecationHandler);
        // this is always expected to match, ParseField is useful for deprecation warnings etc. here
        assert match : "ParseField did not match registered name [" + name + "][" + registryName + "]";
        return value;
    }
}
