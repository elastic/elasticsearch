/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

/**
 * Builds a {@link Settings} that records the name of every setting read from it, by reflectively
 * swapping the instance's internal map with a recording delegate. All typed reads funnel through the
 * map's {@code get}/{@code containsKey}, including absent and defaulted reads. Whole-map access
 * ({@code keySet()}, {@code getByPrefix}, ...) records {@link #ITERATED} instead. The delegate must
 * be a full forwarding wrapper: {@code TreeMap.subMap} views bypass subclass overrides.
 *
 * <p>Test-only; see {@code AnalysisFactoryTestCase}'s sharing contract.
 */
public final class SettingsReadRecorder {

    /** Recorded when the settings are iterated or sliced wholesale rather than read by name. */
    public static final String ITERATED = "<all settings iterated>";

    /** The recording {@link Settings} and the live, sorted set of recorded read names. */
    public record Recorded(Settings settings, SortedSet<String> reads) {}

    private SettingsReadRecorder() {}

    @SuppressForbidden(reason = "test-only reflection: the read-recording map can only be installed by swapping Settings' private map")
    public static Recorded record(Settings source) {
        SortedSet<String> reads = new TreeSet<>();
        // sentinel keeps the instance non-empty; never swap the field on the Settings.EMPTY singleton
        Settings target = Settings.builder().put("__settings_read_recorder__", "").build();
        try {
            Field field = Settings.class.getDeclaredField("settings");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            NavigableMap<String, Object> content = (NavigableMap<String, Object>) field.get(source);
            field.set(target, new RecordingMap(new TreeMap<>(content), reads));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot instrument Settings; did its internal [settings] field change?", e);
        }
        return new Recorded(target, reads);
    }

    private static final class RecordingMap implements NavigableMap<String, Object> {
        private final NavigableMap<String, Object> delegate;
        private final Set<String> reads;

        RecordingMap(NavigableMap<String, Object> delegate, Set<String> reads) {
            this.delegate = delegate;
            this.reads = reads;
        }

        private <T> T iterated(T result) {
            reads.add(ITERATED);
            return result;
        }

        // named reads

        @Override
        public Object get(Object key) {
            reads.add((String) key);
            return delegate.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            reads.add((String) key);
            return delegate.containsKey(key);
        }

        // wholesale access

        @Override
        public Set<Map.Entry<String, Object>> entrySet() {
            return iterated(delegate.entrySet());
        }

        @Override
        public Set<String> keySet() {
            return iterated(delegate.keySet());
        }

        @Override
        public NavigableSet<String> navigableKeySet() {
            return iterated(delegate.navigableKeySet());
        }

        @Override
        public NavigableSet<String> descendingKeySet() {
            return iterated(delegate.descendingKeySet());
        }

        @Override
        public Collection<Object> values() {
            return iterated(delegate.values());
        }

        @Override
        public void forEach(BiConsumer<? super String, ? super Object> action) {
            reads.add(ITERATED);
            delegate.forEach(action);
        }

        @Override
        public NavigableMap<String, Object> descendingMap() {
            return iterated(delegate.descendingMap());
        }

        @Override
        public NavigableMap<String, Object> subMap(String fromKey, boolean fromInclusive, String toKey, boolean toInclusive) {
            return iterated(delegate.subMap(fromKey, fromInclusive, toKey, toInclusive));
        }

        @Override
        public NavigableMap<String, Object> headMap(String toKey, boolean inclusive) {
            return iterated(delegate.headMap(toKey, inclusive));
        }

        @Override
        public NavigableMap<String, Object> tailMap(String fromKey, boolean inclusive) {
            return iterated(delegate.tailMap(fromKey, inclusive));
        }

        @Override
        public java.util.SortedMap<String, Object> subMap(String fromKey, String toKey) {
            return iterated(delegate.subMap(fromKey, toKey));
        }

        @Override
        public java.util.SortedMap<String, Object> headMap(String toKey) {
            return iterated(delegate.headMap(toKey));
        }

        @Override
        public java.util.SortedMap<String, Object> tailMap(String fromKey) {
            return iterated(delegate.tailMap(fromKey));
        }

        // neutral forwarding

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean containsValue(Object value) {
            return delegate.containsValue(value);
        }

        @Override
        public Object put(String key, Object value) {
            return delegate.put(key, value);
        }

        @Override
        public Object remove(Object key) {
            return delegate.remove(key);
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            delegate.putAll(m);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public Comparator<? super String> comparator() {
            return delegate.comparator();
        }

        @Override
        public String firstKey() {
            return delegate.firstKey();
        }

        @Override
        public String lastKey() {
            return delegate.lastKey();
        }

        @Override
        public Map.Entry<String, Object> lowerEntry(String key) {
            return delegate.lowerEntry(key);
        }

        @Override
        public String lowerKey(String key) {
            return delegate.lowerKey(key);
        }

        @Override
        public Map.Entry<String, Object> floorEntry(String key) {
            return delegate.floorEntry(key);
        }

        @Override
        public String floorKey(String key) {
            return delegate.floorKey(key);
        }

        @Override
        public Map.Entry<String, Object> ceilingEntry(String key) {
            return delegate.ceilingEntry(key);
        }

        @Override
        public String ceilingKey(String key) {
            return delegate.ceilingKey(key);
        }

        @Override
        public Map.Entry<String, Object> higherEntry(String key) {
            return delegate.higherEntry(key);
        }

        @Override
        public String higherKey(String key) {
            return delegate.higherKey(key);
        }

        @Override
        public Map.Entry<String, Object> firstEntry() {
            return delegate.firstEntry();
        }

        @Override
        public Map.Entry<String, Object> lastEntry() {
            return delegate.lastEntry();
        }

        @Override
        public Map.Entry<String, Object> pollFirstEntry() {
            return delegate.pollFirstEntry();
        }

        @Override
        public Map.Entry<String, Object> pollLastEntry() {
            return delegate.pollLastEntry();
        }

        @Override
        public boolean equals(Object o) {
            return delegate.equals(o);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}
