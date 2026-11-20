/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A wrapper around the system properties that checks every write.
 * <p>
 * Reads go to the real properties. A write to a key runs the write check for that key first.
 * The key set, value collection and entry set are read-only.
 * <p>
 * Every public method of {@link Properties} must be overridden here. A method that is not
 * overridden would use the empty state of this object, or write without a check.
 */
@SuppressWarnings("serial")
public final class SystemPropertiesWrapper extends Properties {

    private final Supplier<Properties> properties;
    private final InstrumentationRegistry registry;

    /**
     * @param properties returns the real system properties, asked on every call because they can be replaced
     * @param registry checks each write, throws if the write is not allowed
     */
    public SystemPropertiesWrapper(Supplier<Properties> properties, InstrumentationRegistry registry) {
        this.properties = properties;
        this.registry = registry;
    }

    private Properties properties() {
        return properties.get();
    }

    private void checkWrite(Object key) {
        Objects.requireNonNull(key);
        if (key instanceof String name) {
            registry.checkWriteProperty$(name);
        } else {
            throw new IllegalArgumentException("system property keys must be strings");
        }
    }

    // writes

    @Override
    public Object setProperty(String key, String value) {
        checkWrite(key);
        return properties().setProperty(key, value);
    }

    @Override
    public Object put(Object key, Object value) {
        checkWrite(key);
        return properties().put(key, value);
    }

    @Override
    public Object remove(Object key) {
        checkWrite(key);
        return properties().remove(key);
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkWrite(key);
        return properties().remove(key, value);
    }

    @Override
    public void putAll(Map<?, ?> t) {
        // the caller owns the map, so read it once and write only what was checked
        Map<Object, Object> checked = new HashMap<>();
        for (Map.Entry<?, ?> entry : t.entrySet()) {
            Object key = entry.getKey();
            checkWrite(key);
            checked.put(key, entry.getValue());
        }
        properties().putAll(checked);
    }

    @Override
    public void clear() {
        // only allowed if every key may be written, and only the checked keys are removed
        Properties properties = properties();
        List<Object> keys = new ArrayList<>(properties.keySet());
        for (Object key : keys) {
            checkWrite(key);
        }
        for (Object key : keys) {
            properties().remove(key);
        }
    }

    @Override
    public void replaceAll(BiFunction<? super Object, ? super Object, ?> function) {
        properties().replaceAll((key, value) -> {
            checkWrite(key);
            return function.apply(key, value);
        });
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        checkWrite(key);
        return properties().putIfAbsent(key, value);
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        checkWrite(key);
        return properties().replace(key, oldValue, newValue);
    }

    @Override
    public Object replace(Object key, Object value) {
        checkWrite(key);
        return properties().replace(key, value);
    }

    @Override
    public Object computeIfAbsent(Object key, Function<? super Object, ?> mappingFunction) {
        checkWrite(key);
        return properties().computeIfAbsent(key, mappingFunction);
    }

    @Override
    public Object computeIfPresent(Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        checkWrite(key);
        return properties().computeIfPresent(key, remappingFunction);
    }

    @Override
    public Object compute(Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        checkWrite(key);
        return properties().compute(key, remappingFunction);
    }

    @Override
    public Object merge(Object key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        checkWrite(key);
        return properties().merge(key, value, remappingFunction);
    }

    // loads read into a separate object first, so that each loaded key is checked

    @Override
    public void load(Reader reader) throws IOException {
        Properties loaded = new Properties();
        loaded.load(reader);
        putAll(loaded);
    }

    @Override
    public void load(InputStream inStream) throws IOException {
        Properties loaded = new Properties();
        loaded.load(inStream);
        putAll(loaded);
    }

    @Override
    public void loadFromXML(InputStream in) throws IOException {
        Properties loaded = new Properties();
        loaded.loadFromXML(in);
        putAll(loaded);
    }

    // read-only collection views

    @Override
    public Set<Object> keySet() {
        return Collections.unmodifiableSet(properties().keySet());
    }

    @Override
    public Collection<Object> values() {
        return Collections.unmodifiableCollection(properties().values());
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        // the map wrapper also stops writes through the entries
        return Collections.unmodifiableMap(properties()).entrySet();
    }

    // reads

    @Override
    public String getProperty(String key) {
        return properties().getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return properties().getProperty(key, defaultValue);
    }

    @Override
    public Enumeration<?> propertyNames() {
        return properties().propertyNames();
    }

    @Override
    public Set<String> stringPropertyNames() {
        return properties().stringPropertyNames();
    }

    @Override
    public int size() {
        return properties().size();
    }

    @Override
    public boolean isEmpty() {
        return properties().isEmpty();
    }

    @Override
    public Enumeration<Object> keys() {
        return properties().keys();
    }

    @Override
    public Enumeration<Object> elements() {
        return properties().elements();
    }

    @Override
    public boolean contains(Object value) {
        return properties().contains(value);
    }

    @Override
    public boolean containsValue(Object value) {
        return properties().containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        return properties().containsKey(key);
    }

    @Override
    public Object get(Object key) {
        return properties().get(key);
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        return properties().getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super Object, ? super Object> action) {
        properties().forEach(action);
    }

    @Override
    public String toString() {
        return properties().toString();
    }

    @Override
    public boolean equals(Object o) {
        return properties().equals(o);
    }

    @Override
    public int hashCode() {
        return properties().hashCode();
    }

    /**
     * Returns a plain copy. Writes to the copy do not reach the system properties().
     */
    @Override
    public Object clone() {
        return properties().clone();
    }

    // output

    @Override
    @SuppressWarnings("deprecation")
    public void save(OutputStream out, String comments) {
        properties().save(out, comments);
    }

    @Override
    public void store(Writer writer, String comments) throws IOException {
        properties().store(writer, comments);
    }

    @Override
    public void store(OutputStream out, String comments) throws IOException {
        properties().store(out, comments);
    }

    @Override
    public void storeToXML(OutputStream os, String comment) throws IOException {
        properties().storeToXML(os, comment);
    }

    @Override
    public void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
        properties().storeToXML(os, comment, encoding);
    }

    @Override
    public void storeToXML(OutputStream os, String comment, Charset charset) throws IOException {
        properties().storeToXML(os, comment, charset);
    }

    @Override
    public void list(PrintStream out) {
        properties().list(out);
    }

    @Override
    public void list(PrintWriter out) {
        properties().list(out);
    }
}
