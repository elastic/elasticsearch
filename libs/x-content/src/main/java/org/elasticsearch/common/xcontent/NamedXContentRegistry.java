/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

        /** A parser capability of parser the entry's class. */
        private final ContextParser<Object, ?> parser;

        /** Creates a new entry which can be stored by the registry. */
        public <T> Entry(Class<T> categoryClass, ParseField name, CheckedFunction<XContentParser, ? extends T, IOException> parser) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.parser = Objects.requireNonNull((p, c) -> parser.apply(p));
        }
        /**
         * Creates a new entry which can be stored by the registry.
         * Prefer {@link Entry#Entry(Class, ParseField, CheckedFunction)} unless you need a context to carry around while parsing.
         */
        public <T> Entry(Class<T> categoryClass, ParseField name, ContextParser<Object, ? extends T> parser) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.parser = Objects.requireNonNull(parser);
        }
    }

    private final Map<Class<?>, Map<String, Entry>> registry;

    public NamedXContentRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            registry = emptyMap();
            return;
        }
        entries = new ArrayList<>(entries);
        entries.sort((e1, e2) -> e1.categoryClass.getName().compareTo(e2.categoryClass.getName()));

        Map<Class<?>, Map<String, Entry>> registry = new HashMap<>();
        Map<String, Entry> parsers = null;
        Class<?> currentCategory = null;
        for (Entry entry : entries) {
            if (currentCategory != entry.categoryClass) {
                if (currentCategory != null) {
                    // we've seen the last of this category, put it into the big map
                    registry.put(currentCategory, unmodifiableMap(parsers));
                }
                parsers = new HashMap<>();
                currentCategory = entry.categoryClass;
            }

            for (String name : entry.name.getAllNamesIncludedDeprecated()) {
                Object old = parsers.put(name, entry);
                if (old != null) {
                    throw new IllegalArgumentException("NamedXContent [" + currentCategory.getName() + "][" + entry.name + "]" +
                        " is already registered for [" + old.getClass().getName() + "]," +
                        " cannot register [" + entry.parser.getClass().getName() + "]");
                }
            }
        }
        // handle the last category
        registry.put(currentCategory, unmodifiableMap(parsers));

        this.registry = unmodifiableMap(registry);
    }

    /**
     * Parse a named object, throwing an exception if the parser isn't found. Throws an {@link NamedObjectNotFoundException} if the
     * {@code categoryClass} isn't registered because this is almost always a bug. Throws an {@link NamedObjectNotFoundException} if the
     * {@code categoryClass} is registered but the {@code name} isn't.
     *
     * @throws NamedObjectNotFoundException if the categoryClass or name is not registered
     */
    public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
        Map<String, Entry> parsers = registry.get(categoryClass);
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
            throw new XContentParseException(parser.getTokenLocation(),
                    "unable to parse " + categoryClass.getSimpleName() + " with name [" + name + "]: parser didn't match");
        }
        return categoryClass.cast(entry.parser.parse(parser, context));
    }

}
