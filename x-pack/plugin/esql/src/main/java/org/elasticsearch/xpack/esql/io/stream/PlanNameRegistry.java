/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A registry of ESQL names to readers and writers, that can be used to serialize a physical plan
 * fragment. Allows to serialize the non-(Named)Writable types in both the QL and ESQL modules.
 * Serialization is from the outside in, rather than from within.
 * <p>
 * This class is somewhat analogous to NamedWriteableRegistry, but does not require the types to
 * be NamedWriteable.
 */
public class PlanNameRegistry {

    public static final PlanNameRegistry INSTANCE = new PlanNameRegistry();

    /** Adaptable writer interface to bridge between ESQL and regular stream outputs. */
    @FunctionalInterface
    public interface PlanWriter<V> extends Writeable.Writer<V> {

        void write(PlanStreamOutput out, V value) throws IOException;

        @Override
        default void write(StreamOutput out, V value) throws IOException {
            write((PlanStreamOutput) out, value);
        }

        static <V> Writeable.Writer<V> writerFromPlanWriter(PlanWriter<V> planWriter) {
            return planWriter;
        }
    }

    /** Adaptable reader interface to bridge between ESQL and regular stream inputs. */
    @FunctionalInterface
    public interface PlanReader<V> extends Writeable.Reader<V> {

        V read(PlanStreamInput in) throws IOException;

        @Override
        default V read(StreamInput in) throws IOException {
            return read((PlanStreamInput) in);
        }

        static <V> Writeable.Reader<V> readerFromPlanReader(PlanReader<V> planReader) {
            return planReader;
        }
    }

    /** Adaptable reader interface that allows access to the reader name. */
    @FunctionalInterface
    interface PlanNamedReader<V> extends PlanReader<V> {

        V read(PlanStreamInput in, String name) throws IOException;

        default V read(PlanStreamInput in) throws IOException {
            throw new UnsupportedOperationException("should not reach here");
        }
    }

    record Entry(
        /** The superclass of a writeable category will be read by a reader. */
        Class<?> categoryClass,
        /** A name for the writeable which is unique to the categoryClass. */
        String name,
        /** A writer for non-NamedWriteable class */
        PlanWriter<?> writer,
        /** A reader capability of reading the writeable. */
        PlanReader<?> reader
    ) {

        /** Creates a new entry which can be stored by the registry. */
        Entry {
            Objects.requireNonNull(categoryClass);
            Objects.requireNonNull(name);
            Objects.requireNonNull(writer);
            Objects.requireNonNull(reader);
        }

        static <T, C extends T, S extends T> Entry of(
            Class<T> categoryClass,
            Class<C> concreteClass,
            PlanWriter<S> writer,
            PlanReader<S> reader
        ) {
            return new Entry(categoryClass, PlanNamedTypes.name(concreteClass), writer, reader);
        }

        static <T, C extends T, S extends T> Entry of(
            Class<T> categoryClass,
            Class<C> concreteClass,
            PlanWriter<S> writer,
            PlanNamedReader<S> reader
        ) {
            return new Entry(categoryClass, PlanNamedTypes.name(concreteClass), writer, reader);
        }
    }

    /**
     * The underlying data of the registry maps from the category to an inner
     * map of name unique to that category, to the actual reader.
     */
    private final Map<Class<?>, Map<String, PlanReader<?>>> readerRegistry;

    /**
     * The underlying data of the registry maps from the category to an inner
     * map of name unique to that category, to the actual writer.
     */
    private final Map<Class<?>, Map<String, PlanWriter<?>>> writerRegistry;

    public PlanNameRegistry() {
        this(PlanNamedTypes.namedTypeEntries());
    }

    /** Constructs a new registry from the given entries. */
    PlanNameRegistry(List<Entry> entries) {
        entries = new ArrayList<>(entries);
        entries.sort(Comparator.comparing(e -> e.categoryClass().getName()));

        Map<Class<?>, Map<String, PlanReader<?>>> rr = new HashMap<>();
        Map<Class<?>, Map<String, PlanWriter<?>>> wr = new HashMap<>();
        for (Entry entry : entries) {
            Class<?> categoryClass = entry.categoryClass;
            Map<String, PlanReader<?>> readers = rr.computeIfAbsent(categoryClass, v -> new HashMap<>());
            Map<String, PlanWriter<?>> writers = wr.computeIfAbsent(categoryClass, v -> new HashMap<>());

            PlanReader<?> oldReader = readers.put(entry.name, entry.reader);
            if (oldReader != null) {
                throwAlreadyRegisteredReader(categoryClass, entry.name, oldReader.getClass(), entry.reader.getClass());
            }
            PlanWriter<?> oldWriter = writers.put(entry.name, entry.writer);
            if (oldWriter != null) {
                throwAlreadyRegisteredReader(categoryClass, entry.name, oldWriter.getClass(), entry.writer.getClass());
            }
        }

        // add subclass categories, e.g. NamedExpressions are also Expressions
        Map<Class<?>, List<Class<?>>> subCategories = subCategories(entries);
        for (var entry : subCategories.entrySet()) {
            var readers = rr.get(entry.getKey());
            var writers = wr.get(entry.getKey());
            for (Class<?> subCategory : entry.getValue()) {
                readers.putAll(rr.get(subCategory));
                writers.putAll(wr.get(subCategory));
            }
        }

        this.readerRegistry = Map.copyOf(rr);
        this.writerRegistry = Map.copyOf(wr);
    }

    /** Determines the subclass relation of category classes.*/
    static Map<Class<?>, List<Class<?>>> subCategories(List<Entry> entries) {
        Map<Class<?>, Set<Class<?>>> map = new HashMap<>();
        for (Entry entry : entries) {
            Class<?> category = entry.categoryClass;
            for (Entry entry1 : entries) {
                Class<?> category1 = entry1.categoryClass;
                if (category == category1) {
                    continue;
                }
                if (category.isAssignableFrom(category1)) {  // category is a superclass/interface of category1
                    Set<Class<?>> set = map.computeIfAbsent(category, v -> new HashSet<>());
                    set.add(category1);
                }
            }
        }
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, s -> new ArrayList<>(s.getValue())));
    }

    <T> PlanReader<? extends T> getReader(Class<T> categoryClass, String name) {
        Map<String, PlanReader<?>> readers = getReaders(categoryClass);
        return getReader(categoryClass, name, readers);
    }

    static <T> PlanReader<? extends T> getReader(Class<T> categoryClass, String name, Map<String, PlanReader<?>> readers) {
        @SuppressWarnings("unchecked")
        PlanReader<? extends T> reader = (PlanReader<? extends T>) readers.get(name);
        if (reader == null) {
            throwOnUnknownReadable(categoryClass, name);
        }
        return reader;
    }

    <T> Map<String, PlanReader<?>> getReaders(Class<T> categoryClass) {
        Map<String, PlanReader<?>> readers = readerRegistry.get(categoryClass);
        if (readers == null) {
            throwOnUnknownCategory(categoryClass);
        }
        return readers;
    }

    <T> PlanWriter<? extends T> getWriter(Class<T> categoryClass, String name, Map<String, PlanWriter<?>> writers) {
        @SuppressWarnings("unchecked")
        PlanWriter<? extends T> writer = (PlanWriter<? extends T>) writers.get(name);
        if (writer == null) {
            throwOnUnknownWritable(categoryClass, name);
        }
        return writer;
    }

    public <T> Map<String, PlanWriter<?>> getWriters(Class<T> categoryClass) {
        Map<String, PlanWriter<?>> writers = writerRegistry.get(categoryClass);
        if (writers == null) {
            throwOnUnknownCategory(categoryClass);
        }
        return writers;
    }

    public <T> PlanWriter<? extends T> getWriter(Class<T> categoryClass, String name) {
        Map<String, PlanWriter<?>> writers = getWriters(categoryClass);
        return getWriter(categoryClass, name, writers);
    }

    private static void throwAlreadyRegisteredReader(Class<?> categoryClass, String entryName, Class<?> oldReader, Class<?> entryReader) {
        throw new IllegalArgumentException(
            "PlanReader ["
                + categoryClass.getName()
                + "]["
                + entryName
                + "]"
                + " is already registered for ["
                + oldReader.getName()
                + "],"
                + " cannot register ["
                + entryReader.getName()
                + "]"
        );
    }

    private static <T> void throwOnUnknownWritable(Class<T> categoryClass, String name) {
        throw new IllegalArgumentException("Unknown writeable [" + categoryClass.getName() + "][" + name + "]");
    }

    private static <T> void throwOnUnknownCategory(Class<T> categoryClass) {
        throw new IllegalArgumentException("Unknown writeable category [" + categoryClass.getName() + "]");
    }

    private static <T> void throwOnUnknownReadable(Class<T> categoryClass, String name) {
        throw new IllegalArgumentException("Unknown readable [" + categoryClass.getName() + "][" + name + "]");
    }
}
