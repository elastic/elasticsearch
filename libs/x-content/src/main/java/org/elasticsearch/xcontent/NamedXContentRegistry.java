/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class NamedXContentRegistry {
    /**
     * The empty {@link NamedXContentRegistry} for use when you are sure that you aren't going to call
     * {@link XContentParser#namedObject(Class, String, Object)}. Be *very* careful with this singleton because a parser using it will fail
     * every call to {@linkplain XContentParser#namedObject(Class, String, Object)}. Every non-test usage really should be checked
     * thoroughly and marked with a comment about how it was checked. That way anyone that sees code that uses it knows that it is
     * potentially dangerous.
     */
    public static final NamedXContentRegistry EMPTY = new NamedXContentRegistry(emptyList());

    /**
     * An entry in the {@linkplain NamedXContentRegistry} containing the name of the object and the parser that can parse it.
     */
    public static class Entry {
        /** The class that this entry can read. */
        public final Class<?> categoryClass;

        /** A name for the entry which is unique within the {@link #categoryClass}. */
        public final ParseField name;

        public final Function<RestApiVersion, Boolean> restApiCompatibility;

        /** A parser capability of parser the entry's class. */
        private final ContextParser<Object, ?> parser;

        /**
         * Creates a new entry which can be stored by the registry.
         */
        public <T> Entry(Class<T> categoryClass, ParseField name, CheckedFunction<XContentParser, ? extends T, IOException> parser) {
            this(categoryClass, name, (p, c) -> parser.apply(p), name.getForRestApiVersion());
        }

        public <T> Entry(
            Class<T> categoryClass,
            ParseField name,
            CheckedFunction<XContentParser, ? extends T, IOException> parser,
            Function<RestApiVersion, Boolean> restApiCompatibility
        ) {
            this(categoryClass, name, (p, c) -> parser.apply(p), restApiCompatibility);
        }

        /**
         * Creates a new entry which can be stored by the registry.
         * Prefer {@link Entry#Entry(Class, ParseField, CheckedFunction)} unless you need a context to carry around while parsing.
         */
        public <T> Entry(Class<T> categoryClass, ParseField name, ContextParser<Object, ? extends T> parser) {
            this(categoryClass, name, parser, name.getForRestApiVersion());
        }

        public <T> Entry(
            Class<T> categoryClass,
            ParseField name,
            ContextParser<Object, ? extends T> parser,
            Function<RestApiVersion, Boolean> restApiCompatibility
        ) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.parser = Objects.requireNonNull(parser);
            this.restApiCompatibility = restApiCompatibility;
        }
    }

    private final Map<RestApiVersion, Map<Class<?>, Map<String, Entry>>> registry;

    public NamedXContentRegistry(List<Entry> entries) {
        this.registry = unmodifiableMap(createRegistry(entries));
    }

    private static Map<RestApiVersion, Map<Class<?>, Map<String, Entry>>> createRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            return emptyMap();
        }

        Map<RestApiVersion, Map<Class<?>, Map<String, Entry>>> newRegistry = new HashMap<>();
        for (Entry entry : entries) {
            for (String name : entry.name.getAllNamesIncludedDeprecated()) {
                if (RestApiVersion.minimumSupported().matches(entry.restApiCompatibility)) {
                    registerParsers(newRegistry, entry, name, RestApiVersion.minimumSupported());
                }
                if (RestApiVersion.current().matches(entry.restApiCompatibility)) {
                    registerParsers(newRegistry, entry, name, RestApiVersion.current());
                }
            }
        }
        return newRegistry;
    }

    private static void registerParsers(
        Map<RestApiVersion, Map<Class<?>, Map<String, Entry>>> newRegistry,
        Entry entry,
        String name,
        RestApiVersion restApiVersion
    ) {
        final Map<Class<?>, Map<String, Entry>> classRegistry = newRegistry.computeIfAbsent(restApiVersion, (v) -> new HashMap<>());
        final Map<String, Entry> parsers = classRegistry.computeIfAbsent(entry.categoryClass, (v) -> new HashMap<>());
        Object old = parsers.put(name, entry);
        if (old != null) {
            throw new IllegalArgumentException(
                "NamedXContent ["
                    + entry.categoryClass.getName()
                    + "]["
                    + entry.name
                    + "]"
                    + " is already registered for ["
                    + old.getClass().getName()
                    + "],"
                    + " cannot register ["
                    + entry.parser.getClass().getName()
                    + "]"
            );
        }
    }

    /**
     * Parse a named object, throwing an exception if the parser isn't found. Throws an {@link NamedObjectNotFoundException} if the
     * {@code categoryClass} isn't registered because this is almost always a bug. Throws an {@link NamedObjectNotFoundException} if the
     * {@code categoryClass} is registered but the {@code name} isn't.
     *
     * @throws NamedObjectNotFoundException if the categoryClass or name is not registered
     */
    public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
        Entry entry = lookupParser(categoryClass, name, parser);
        return categoryClass.cast(entry.parser.parse(parser, context));
    }

    // scope for testing
    public <T> Entry lookupParser(Class<T> categoryClass, String name, XContentParser parser) {
        Map<String, Entry> parsers = registry.getOrDefault(parser.getRestApiVersion(), emptyMap()).get(categoryClass);
        if (parsers == null) {
            if (registry.isEmpty()) {
                // The "empty" registry will never work so we throw a better exception as a hint.
                throw new XContentParseException("named objects are not supported for this parser");
            }
            throw new XContentParseException("unknown named object category [" + categoryClass.getName() + "]");
        }
        Entry entry = parsers.get(name);
        if (entry == null) {
            throw new NamedObjectNotFoundException(parser.getTokenLocation(), "unknown field [" + name + "]", parsers.keySet());
        }
        if (false == entry.name.match(name, parser.getDeprecationHandler())) {
            /* Note that this shouldn't happen because we already looked up the entry using the names but we need to call `match` anyway
             * because it is responsible for logging deprecation warnings. */
            throw new XContentParseException(
                parser.getTokenLocation(),
                "unable to parse " + categoryClass.getSimpleName() + " with name [" + name + "]: parser didn't match"
            );
        }
        return entry;
    }

}
