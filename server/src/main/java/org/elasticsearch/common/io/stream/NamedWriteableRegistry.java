/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A registry for {@link org.elasticsearch.common.io.stream.Writeable.Reader} readers of {@link NamedWriteable}.
 *
 * The registration is keyed by the combination of the category class of {@link NamedWriteable}, and a name unique
 * to that category.
 */
public class NamedWriteableRegistry {

    /** An entry in the registry, made up of a category class and name, and a reader for that category class. */
    public static class Entry {

        /** The superclass of a {@link NamedWriteable} which will be read by {@link #reader}. */
        public final Class<?> categoryClass;

        /** A name for the writeable which is unique to the {@link #categoryClass}. */
        public final String name;

        /** A reader capability of reading*/
        public final Writeable.Reader<?> reader;

        /** Creates a new entry which can be stored by the registry. */
        public <T extends NamedWriteable> Entry(Class<T> categoryClass, String name, Writeable.Reader<? extends T> reader) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.reader = Objects.requireNonNull(reader);
        }
    }

    /**
     * The underlying data of the registry maps from the category to an inner
     * map of name unique to that category, to the actual reader.
     */
    private final Map<Class<?>, Map<String, Writeable.Reader<?>>> registry;

    /**
     * Constructs a new registry from the given entries.
     */
    @SuppressWarnings("rawtypes")
    public NamedWriteableRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            registry = Map.of();
            return;
        }
        entries = new ArrayList<>(entries);
        entries.sort(Comparator.comparing(e -> e.categoryClass.getName()));

        Map<Class<?>, Map<String, Writeable.Reader<?>>> registry = new HashMap<>();
        Map<String, Writeable.Reader<?>> readers = null;
        Class currentCategory = null;
        for (Entry entry : entries) {
            if (currentCategory != entry.categoryClass) {
                if (currentCategory != null) {
                    // we've seen the last of this category, put it into the big map
                    registry.put(currentCategory, Map.copyOf(readers));
                }
                readers = new HashMap<>();
                currentCategory = entry.categoryClass;
            }

            Writeable.Reader<?> oldReader = readers.put(entry.name, entry.reader);
            if (oldReader != null) {
                throw new IllegalArgumentException(
                    "NamedWriteable ["
                        + currentCategory.getName()
                        + "]["
                        + entry.name
                        + "]"
                        + " is already registered for ["
                        + oldReader.getClass().getName()
                        + "],"
                        + " cannot register ["
                        + entry.reader.getClass().getName()
                        + "]"
                );
            }
        }
        // handle the last category
        registry.put(currentCategory, Map.copyOf(readers));

        this.registry = Map.copyOf(registry);
    }

    /**
     * Returns a reader for a {@link NamedWriteable} object identified by the
     * name provided as argument and its category.
     */
    public <T> Writeable.Reader<? extends T> getReader(Class<T> categoryClass, String name) {
        Map<String, Writeable.Reader<?>> readers = getReaders(categoryClass);
        return getReader(categoryClass, name, readers);
    }

    /**
     * @param categoryClass category of the reader
     * @param name          name of the writeable
     * @param readers       map of readers for the category
     * @return reader for the named writable of the given {@code name}
     */
    public static <T> Writeable.Reader<? extends T> getReader(
        Class<T> categoryClass,
        String name,
        Map<String, Writeable.Reader<?>> readers
    ) {
        @SuppressWarnings("unchecked")
        Writeable.Reader<? extends T> reader = (Writeable.Reader<? extends T>) readers.get(name);
        if (reader == null) {
            throwOnUnknownWritable(categoryClass, name);
        }
        return reader;
    }

    /**
     * Gets the readers map keyed by name for the given category
     * @param categoryClass category to get readers map for
     * @return map of readers for the category
     */
    public <T> Map<String, Writeable.Reader<?>> getReaders(Class<T> categoryClass) {
        Map<String, Writeable.Reader<?>> readers = registry.get(categoryClass);
        if (readers == null) {
            throwOnUnknownCategory(categoryClass);
        }
        return readers;
    }

    private static <T> void throwOnUnknownWritable(Class<T> categoryClass, String name) {
        throw new IllegalArgumentException("Unknown NamedWriteable [" + categoryClass.getName() + "][" + name + "]");
    }

    private static <T> void throwOnUnknownCategory(Class<T> categoryClass) {
        throw new IllegalArgumentException("Unknown NamedWriteable category [" + categoryClass.getName() + "]");
    }
}
