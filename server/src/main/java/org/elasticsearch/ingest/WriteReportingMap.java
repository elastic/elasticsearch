/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * {@link Map} implementation for use in {@link WriteReportingWrapper}. This reports write mutations directly on this map, or on derived
 * objects such as those returned by calling {@link #keySet} or by calling {@code iterator} on {@link #keySet}. It does
 * <strong>not</strong> report mutations on values contained within the map — although the caller may populate the wrapped map with values
 * which report their own mutations.
 */
public class WriteReportingMap<K, V> implements Map<K, V> {

    private final Map<K, V> delegate;
    private final Consumer<String> reporter;
    private final String label;

    public WriteReportingMap(Map<K, V> delegate, Consumer<String> reporter, String label) {
        this.delegate = requireNonNull(delegate);
        this.reporter = requireNonNull(reporter);
        this.label = label;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return delegate.get(key);
    }

    @Override
    public V put(K key, V value) {
        reporter.accept(label + ".put(K, V)");
        return delegate.put(key, value);
    }

    @Override
    public V remove(Object key) {
        reporter.accept(label + ".remove(Object)");
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        reporter.accept(label + ".putAll(Map<? extends K, ? extends V>)");
        delegate.putAll(m);
    }

    @Override
    public void clear() {
        reporter.accept(label + ".clear()");
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return new WriteReportingSet<>(delegate.keySet(), reporter, label + ".keySet()");
    }

    @Override
    public Collection<V> values() {
        return new WriteReportingValueCollection(delegate.values());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new WriteReportingEntrySet(delegate.entrySet());
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return delegate.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        delegate.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        reporter.accept(label + ".replaceAll(BiFunction<? super K, ? super V, ? extends V>)");
        delegate.replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        reporter.accept(label + ".putIfAbsent(K, V)");
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        reporter.accept(label + ".remove(Object, Object)");
        return delegate.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        reporter.accept(label + ".replace(K, V, V)");
        return delegate.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        reporter.accept(label + ".replace(K, V)");
        return delegate.replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        reporter.accept(label + ".computeIfAbsent(K, Function<? super K, ? extends V>)");
        return delegate.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        reporter.accept(label + ".computeIfPresent(K, BiFunction<? super K, ? super V, ? extends V>)");
        return delegate.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        reporter.accept(label + ".compute(K, BiFunction<? super K, ? super V, ? extends V>)");
        return delegate.compute(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        reporter.accept(label + ".merge(K, V, BiFunction<? super V, ? super V, ? extends V>)");
        return delegate.merge(key, value, remappingFunction);
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

    private class WriteReportingValueCollection implements Collection<V> {

        private final Collection<V> valuesDelegate;

        WriteReportingValueCollection(Collection<V> valuesDelegate) {
            this.valuesDelegate = valuesDelegate;
        }

        @Override
        public int size() {
            return valuesDelegate.size();
        }

        @Override
        public boolean isEmpty() {
            return valuesDelegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return valuesDelegate.contains(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new WriteReportingValueCollectionIterator(valuesDelegate.iterator());
        }

        @Override
        public void forEach(Consumer<? super V> action) {
            valuesDelegate.forEach(action);
        }

        @Override
        public Object[] toArray() {
            return valuesDelegate.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return valuesDelegate.toArray(a);
        }

        @Override
        public <T> T[] toArray(IntFunction<T[]> generator) {
            return valuesDelegate.toArray(generator);
        }

        @Override
        public boolean add(V v) {
            // Value collection does not support add, but let's report it and forward to the delegate to let that throw
            reporter.accept(label + ".values().add(V)");
            return valuesDelegate.add(v);
        }

        @Override
        public boolean remove(Object o) {
            reporter.accept(label + ".values().remove(Object)");
            return valuesDelegate.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return valuesDelegate.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            // Value collection does not support add, but let's report it and forward to the delegate to let that throw
            reporter.accept(label + ".values().addAll(Collection<? extends V>)");
            return valuesDelegate.addAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            reporter.accept(label + ".values().removeAll(Collection<?>)");
            return valuesDelegate.removeAll(c);
        }

        @Override
        public boolean removeIf(Predicate<? super V> filter) {
            reporter.accept(label + ".values().removeIf(Predicate<? super V>)");
            return valuesDelegate.removeIf(filter);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            reporter.accept(label + ".values().retainAll(Collection<?>)");
            return valuesDelegate.retainAll(c);
        }

        @Override
        public void clear() {
            reporter.accept(label + ".values().clear()");
            valuesDelegate.clear();
        }

        @Override
        public Spliterator<V> spliterator() {
            return valuesDelegate.spliterator();
        }

        @Override
        public Stream<V> stream() {
            return valuesDelegate.stream();
        }

        @Override
        public Stream<V> parallelStream() {
            return valuesDelegate.parallelStream();
        }

        @Override
        public boolean equals(Object o) {
            return valuesDelegate.equals(o);
        }

        @Override
        public int hashCode() {
            return valuesDelegate.hashCode();
        }

        @Override
        public String toString() {
            return valuesDelegate.toString();
        }
    }

    private class WriteReportingValueCollectionIterator implements Iterator<V> {

        private final Iterator<V> valuesIteratorDelegate;

        WriteReportingValueCollectionIterator(Iterator<V> valuesIteratorDelegate) {
            this.valuesIteratorDelegate = valuesIteratorDelegate;
        }

        @Override
        public boolean hasNext() {
            return valuesIteratorDelegate.hasNext();
        }

        @Override
        public V next() {
            return valuesIteratorDelegate.next();
        }

        @Override
        public void remove() {
            reporter.accept(label + ".values().iterator()...remove()");
            valuesIteratorDelegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super V> action) {
            valuesIteratorDelegate.forEachRemaining(action);
        }
    }

    // N.B. We cannot use the general-purpose WriteReportingSet here, because that requires that its elements are themselves write-reporting
    // if required, and the entries in the wrapped entry set are not. We need an implementation which wraps the entries as needed.
    private class WriteReportingEntrySet implements Set<Map.Entry<K, V>> {

        private final Set<Entry<K, V>> entrySetDelegate;

        WriteReportingEntrySet(Set<Entry<K, V>> entrySetDelegate) {
            this.entrySetDelegate = entrySetDelegate;
        }

        @Override
        public int size() {
            return entrySetDelegate.size();
        }

        @Override
        public boolean isEmpty() {
            return entrySetDelegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return entrySetDelegate.contains(o);
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new WriteReportingEntrySetIterator(entrySetDelegate.iterator());
        }

        @Override
        public void forEach(Consumer<? super Entry<K, V>> action) {
            entrySetDelegate.forEach(e -> action.accept(new WriteReportingEntry(e)));
        }

        @Override
        public Object[] toArray() {
            Object[] array = entrySetDelegate.toArray();
            wrapEntryArray(array);
            return array;
        }

        @Override
        public <T> T[] toArray(T[] a) {
            T[] array = entrySetDelegate.toArray(a);
            wrapEntryArray(array);
            return array;
        }

        @Override
        public <T> T[] toArray(IntFunction<T[]> generator) {
            T[] array = entrySetDelegate.toArray(generator);
            wrapEntryArray(array);
            return array;
        }

        /**
         * Takes an array, which must contain instances of {@code Map.Entry<K, V>}, and replaces each entry by wrapping it in a
         * {@link WriteReportingEntry}.
         */
        @SuppressWarnings("unchecked") // The two casts here will be safe as long at the array passed in is of a valid type
        private <T> void wrapEntryArray(T[] array) {
            for (int i = 0; i < array.length; i++) {
                Map.Entry<K, V> original = (Map.Entry<K, V>) array[i];
                if (original == null) {
                    // The contract of Set.toArray(T[]) says: "If this set fits in the specified array with room to spare (i.e., the array
                    // has more elements than this set), the element in the array immediately following the end of the set is set to null."
                    // This is the only way we can encounter a null array element. So, at this point, we know we have wrapped all the
                    // entries from the entry set, and all remaining elements were whatever was in the over-sided array beforehand. And so
                    // we can stop wrapping here.
                    break;
                }
                T wrapped = (T) new WriteReportingEntry(original);
                array[i] = wrapped;
            }
        }

        @Override
        public boolean add(Entry<K, V> kvEntry) {
            // Entry set does not support add, but let's report it and forward to the delegate to let that throw
            reporter.accept(label + ".entrySet().add(Entry<K, V>)");
            return entrySetDelegate.add(kvEntry);
        }

        @Override
        public boolean remove(Object o) {
            reporter.accept(label + ".entrySet().remove(Object)");
            return entrySetDelegate.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return entrySetDelegate.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Entry<K, V>> c) {
            // Entry set does not support add, but let's report it and forward to the delegate to let that throw
            reporter.accept(label + ".entrySet().addAll(Collection<? extends Entry<K, V>>)");
            return entrySetDelegate.addAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            reporter.accept(label + ".entrySet().retainAll(Collection<?>)");
            return entrySetDelegate.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            reporter.accept(label + ".entrySet().removeAll(Collection<?>)");
            return entrySetDelegate.removeAll(c);
        }

        @Override
        public boolean removeIf(Predicate<? super Entry<K, V>> filter) {
            reporter.accept(label + ".entrySet().removeIf(Predicate<? super Entry<K, V>>)");
            return entrySetDelegate.removeIf(filter);
        }

        @Override
        public void clear() {
            reporter.accept(label + ".entrySet().clear()");
            entrySetDelegate.clear();
        }

        @Override
        public Spliterator<Entry<K, V>> spliterator() {
            return new WriteReportingEntrySetSpliterator(entrySetDelegate.spliterator());
        }

        @Override
        public Stream<Entry<K, V>> stream() {
            return entrySetDelegate.stream().map(WriteReportingEntry::new);
        }

        @Override
        public Stream<Entry<K, V>> parallelStream() {
            return entrySetDelegate.parallelStream().map(WriteReportingEntry::new);
        }

        @Override
        public boolean equals(Object o) {
            return entrySetDelegate.equals(o);
        }

        @Override
        public int hashCode() {
            return entrySetDelegate.hashCode();
        }

        @Override
        public String toString() {
            return entrySetDelegate.toString();
        }
    }

    private class WriteReportingEntrySetIterator implements Iterator<Map.Entry<K, V>> {

        private final Iterator<Entry<K, V>> entrySetIteratorDelegate;

        WriteReportingEntrySetIterator(Iterator<Entry<K, V>> entrySetIteratorDelegate) {
            this.entrySetIteratorDelegate = entrySetIteratorDelegate;
        }

        @Override
        public boolean hasNext() {
            return entrySetIteratorDelegate.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            return new WriteReportingEntry(entrySetIteratorDelegate.next());
        }

        @Override
        public void remove() {
            reporter.accept(label + ".entrySet().iterator()...remove()");
            entrySetIteratorDelegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super Entry<K, V>> action) {
            entrySetIteratorDelegate.forEachRemaining(e -> action.accept(new WriteReportingEntry(e)));
        }
    }

    private class WriteReportingEntrySetSpliterator implements Spliterator<Entry<K, V>> {

        private final Spliterator<Entry<K, V>> entrySetSpliteratorDelegate;

        WriteReportingEntrySetSpliterator(Spliterator<Entry<K, V>> entrySetSpliteratorDelegate) {
            this.entrySetSpliteratorDelegate = entrySetSpliteratorDelegate;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Entry<K, V>> action) {
            return entrySetSpliteratorDelegate.tryAdvance(e -> action.accept(new WriteReportingEntry(e)));
        }

        @Override
        public void forEachRemaining(Consumer<? super Entry<K, V>> action) {
            entrySetSpliteratorDelegate.forEachRemaining(e -> action.accept(new WriteReportingEntry(e)));
        }

        @Override
        public Spliterator<Entry<K, V>> trySplit() {
            return new WriteReportingEntrySetSpliterator(entrySetSpliteratorDelegate.trySplit());
        }

        @Override
        public long estimateSize() {
            return entrySetSpliteratorDelegate.estimateSize();
        }

        @Override
        public long getExactSizeIfKnown() {
            return entrySetSpliteratorDelegate.getExactSizeIfKnown();
        }

        @Override
        public int characteristics() {
            return entrySetSpliteratorDelegate.characteristics();
        }

        @Override
        public boolean hasCharacteristics(int characteristics) {
            return entrySetSpliteratorDelegate.hasCharacteristics(characteristics);
        }

        @Override
        public Comparator<? super Entry<K, V>> getComparator() {
            return entrySetSpliteratorDelegate.getComparator();
        }
    }

    private class WriteReportingEntry implements Entry<K, V> {

        private final Entry<K, V> entryDelegate;

        WriteReportingEntry(Entry<K, V> entryDelegate) {
            this.entryDelegate = entryDelegate;
        }

        @Override
        public K getKey() {
            return entryDelegate.getKey();
        }

        @Override
        public V getValue() {
            return entryDelegate.getValue();
        }

        @Override
        public V setValue(V value) {
            reporter.accept(label + ".entrySet()...setValue(V)");
            return entryDelegate.setValue(value);
        }

        @Override
        public boolean equals(Object o) {
            return entryDelegate.equals(o);
        }

        @Override
        public int hashCode() {
            return entryDelegate.hashCode();
        }

        @Override
        public String toString() {
            return entryDelegate.toString();
        }
    }
}
