/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class WriteReportingMapTests extends ESTestCase {

    public void testGet() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        assertThat(wrapper.get("two"), is(2));
        assertThat(reports, empty());
    }

    public void testPut() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        assertThat(wrapper.put("two", 22), is(2));
        assertThat(delegate, is(Map.of("one", 1, "two", 22, "three", 3)));
        assertThat(reports, contains("someMap.put(K, V)"));
    }

    public void testEquals() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        assertThat(wrapper.equals(Map.copyOf(delegate)), is(true));
        assertThat(wrapper.equals(Map.of("one", 1, "two", 2, "three", 33)), is(false));
        assertThat(wrapper.equals(null), is(false));
        assertThat(reports, empty());
    }

    public void testHashCode() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        assertThat(wrapper.hashCode(), is(delegate.hashCode()));
        assertThat(reports, empty());
    }

    public void testToString() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        assertThat(wrapper.toString(), is(delegate.toString()));
        assertThat(reports, empty());
    }

    public void testConstructor_nullDelegate() {
        List<String> reports = new ArrayList<>();
        assertThrows(NullPointerException.class, () -> new WriteReportingMap<>(null, reports::add, "someMap"));
        assertThat(reports, empty());
    }

    public void testConstructor_nullReporter() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        assertThrows(NullPointerException.class, () -> new WriteReportingMap<>(delegate, null, "someMap"));
    }

    public void testKeySet_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<String> keySet = wrapper.keySet();
        assertThat(keySet, containsInAnyOrder("one", "two", "three"));
        assertThat(reports, empty());
    }

    public void testKeySet_remove() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<String> keySet = wrapper.keySet();
        assertThat(keySet.remove("two"), is(true));
        assertThat(delegate, is(Map.of("one", 1, "three", 3)));
        assertThat(reports, contains("someMap.keySet().remove(Object)"));
    }

    public void testKeySet_add_throws() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<String> keySet = wrapper.keySet();
        assertThrows(UnsupportedOperationException.class, () -> keySet.add("illegal")); // key set does not support add
        assertThat(reports, contains("someMap.keySet().add(E)"));
    }

    public void testKeySet_iterator_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<String> iterator = wrapper.keySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalKeys.get(0)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalKeys.get(1)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalKeys.get(2)));
        assertThat(iterator.hasNext(), is(false));
        assertThat(reports, empty());
    }

    public void testKeySet_iterator_remove() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<String> iterator = wrapper.keySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalKeys.get(0)));
        iterator.remove();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalKeys.get(1)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalKeys.get(2)));
        iterator.remove();
        assertThat(iterator.hasNext(), is(false));
        assertThat(delegate, is(Map.of(originalKeys.get(1), originalValues.get(1))));
        assertThat(reports, contains("someMap.keySet().iterator()...remove()", "someMap.keySet().iterator()...remove()"));
    }

    public void testKeySet_equals() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<String> keySet = wrapper.keySet();
        assertThat(keySet.equals(Map.copyOf(delegate).keySet()), is(true));
        assertThat(keySet.equals(Set.of("one", "two", "something else")), is(false));
        assertThat(keySet.equals(null), is(false));
        assertThat(reports, empty());
    }

    public void testKeySet_hashCode() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<String> keySet = wrapper.keySet();
        assertThat(keySet.hashCode(), is(delegate.keySet().hashCode()));
        assertThat(reports, empty());
    }

    public void testKeySet_toString() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<String> keySet = wrapper.keySet();
        assertThat(keySet.toString(), is(delegate.keySet().toString()));
        assertThat(reports, empty());
    }

    public void testValues_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Collection<Integer> values = wrapper.values();
        assertThat(values, containsInAnyOrder(1, 2, 3));
        assertThat(reports, empty());
    }

    public void testValues_remove() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Collection<Integer> values = wrapper.values();
        assertThat(values.remove(2), is(true));
        assertThat(delegate, is(Map.of("one", 1, "three", 3)));
        assertThat(reports, contains("someMap.values().remove(Object)"));
    }

    public void testValues_add_throws() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Collection<Integer> values = wrapper.values();
        assertThrows(UnsupportedOperationException.class, () -> values.add(99)); // values collection does not support add
        assertThat(reports, contains("someMap.values().add(V)"));
    }

    public void testValues_iterator_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Integer> iterator = wrapper.values().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalValues.get(0)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalValues.get(1)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalValues.get(2)));
        assertThat(iterator.hasNext(), is(false));
        assertThat(reports, empty());
    }

    public void testValues_iterator_remove() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Integer> iterator = wrapper.values().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalValues.get(0)));
        iterator.remove();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalValues.get(1)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalValues.get(2)));
        iterator.remove();
        assertThat(iterator.hasNext(), is(false));
        assertThat(delegate, is(Map.of(originalKeys.get(1), originalValues.get(1))));
        assertThat(reports, contains("someMap.values().iterator()...remove()", "someMap.values().iterator()...remove()"));
    }

    public void testValues_equals() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Collection<Integer> values = wrapper.values();
        assertThat(values.equals(Map.copyOf(delegate).values()), is(true));
        assertThat(values.equals(List.of(1, 2, 999)), is(false));
        assertThat(values.equals(null), is(false));
        assertThat(reports, empty());
    }

    public void testValues_hashCode() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Collection<Integer> values = wrapper.values();
        assertThat(values.hashCode(), is(delegate.values().hashCode()));
        assertThat(reports, empty());
    }

    public void testValues_toString() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Collection<Integer> values = wrapper.values();
        assertThat(values.toString(), is(delegate.values().toString()));
        assertThat(reports, empty());
    }

    public void testEntrySet_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThat(entrySet, containsInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_remove() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThat(entrySet.remove(Map.entry("two", 2)), is(true));
        assertThat(entrySet, containsInAnyOrder(Map.entry("one", 1), Map.entry("three", 3)));
        assertThat(delegate, is(Map.of("one", 1, "three", 3)));
        assertThat(reports, contains("someMap.entrySet().remove(Object)"));
    }

    public void testEntrySet_add_throws() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThrows(UnsupportedOperationException.class, () -> entrySet.add(Map.entry("illegal", 999)));
        assertThat(reports, contains("someMap.entrySet().add(Entry<K, V>)"));
    }

    public void testEntrySet_forEach_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        entrySet.forEach(entriesCollected::add);
        assertThat(entriesCollected, containsInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_forEach_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        entrySet.forEach(e -> e.setValue(e.getValue() * 10));
        assertThat(delegate, is(Map.of("one", 10, "two", 20, "three", 30)));
        assertThat(
            reports,
            contains("someMap.entrySet()...setValue(V)", "someMap.entrySet()...setValue(V)", "someMap.entrySet()...setValue(V)")
        );
    }

    public void testEntrySet_toArray_noArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Object[] array = entrySet.toArray();
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_noArg_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Object[] array = entrySet.toArray();
        @SuppressWarnings("unchecked") // safe by the contract of entrySet() and toArray(), assuming the implementations are correct
        Map.Entry<String, Integer> index1Entry = (Map.Entry<String, Integer>) array[1];
        index1Entry.setValue(999);
        assertThat(
            array,
            arrayContaining(
                Map.entry(originalKeys.get(0), originalValues.get(0)),
                Map.entry(originalKeys.get(1), 999),
                Map.entry(originalKeys.get(2), originalValues.get(2))
            )
        );
        assertThat(reports, contains("someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_toArray_rightSizeObjectArrayArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Object[] input = new Object[3];
        Object[] array = entrySet.toArray(input);
        assertThat(array, sameInstance(input));
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_rightSizeEntryArrayArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings({ "unchecked", "rawtypes" }) // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] input = new Map.Entry[3];
        Map.Entry<String, Integer>[] array = entrySet.toArray(input);
        assertThat(array, sameInstance(input));
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_rightSizeEntryArrayArg_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings({ "unchecked", "rawtypes" }) // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] input = new Map.Entry[3];
        Map.Entry<String, Integer>[] array = entrySet.toArray(input);
        array[1].setValue(999);
        assertThat(array, sameInstance(input));
        assertThat(
            array,
            arrayContaining(
                Map.entry(originalKeys.get(0), originalValues.get(0)),
                Map.entry(originalKeys.get(1), 999),
                Map.entry(originalKeys.get(2), originalValues.get(2))
            )
        );
        assertThat(reports, contains("someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_toArray_underSizeEntryArrayArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings({ "unchecked", "rawtypes" }) // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] input = new Map.Entry[2];
        Map.Entry<String, Integer>[] array = entrySet.toArray(input);
        assertThat(array, not(sameInstance(input)));
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_overSizeEntryArrayArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings({ "unchecked", "rawtypes" }) // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] input = new Map.Entry[4];
        Map.Entry<String, Integer>[] array = entrySet.toArray(input);
        assertThat(array, sameInstance(input));
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3), null));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_overSizeEntryArrayArg_readOnly_nullsElementFollowingSetAndThenLeavesRemainderUntouched() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings({ "unchecked", "rawtypes" }) // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] input = new Map.Entry[] {
            Map.entry("1st entry should be replaced by instance from entry set", 999),
            Map.entry("2nd entry should be replaced by instance from entry set", 999),
            Map.entry("3rd entry should be replaced by instance from entry set", 999),
            Map.entry("4th entry should be replaced by null, per the contract of Set.toArray(T[])", 999),
            Map.entry("5th entry should be left untouched", 999) };
        Map.Entry<String, Integer>[] array = entrySet.toArray(input);
        assertThat(array, sameInstance(input));
        assertThat(
            array,
            arrayContaining(
                Map.entry(originalKeys.get(0), originalValues.get(0)),
                Map.entry(originalKeys.get(1), originalValues.get(1)),
                Map.entry(originalKeys.get(2), originalValues.get(2)),
                null,
                Map.entry("5th entry should be left untouched", 999)
            )
        );
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_badTypeArrayArg_throwsCorrectException() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        String[] input = new String[3];
        assertThrows(ArrayStoreException.class, () -> entrySet.toArray(input));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_objectArrayFactoryArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Object[] array = entrySet.toArray(Object[]::new);
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_entryArrayFactoryArg_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings("unchecked") // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] array = (Map.Entry<String, Integer>[]) entrySet.toArray(Map.Entry[]::new);
        assertThat(array, arrayContainingInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_toArray_entryArrayFactoryArg_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        @SuppressWarnings("unchecked") // arrays of generics are just messed up like this
        Map.Entry<String, Integer>[] array = entrySet.toArray(Map.Entry[]::new);
        array[1].setValue(999);
        assertThat(
            array,
            arrayContaining(
                Map.entry(originalKeys.get(0), originalValues.get(0)),
                Map.entry(originalKeys.get(1), 999),
                Map.entry(originalKeys.get(2), originalValues.get(2))
            )
        );
        assertThat(reports, contains("someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_toArray_badTypeArrayFactoryArg_throwsCorrectException() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThrows(ArrayStoreException.class, () -> entrySet.toArray(String[]::new));
        assertThat(reports, empty());
    }

    public void testEntrySet_iterator_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Map.Entry<String, Integer>> iterator = wrapper.entrySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(0), originalValues.get(0))));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(1), originalValues.get(1))));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(2), originalValues.get(2))));
        assertThat(iterator.hasNext(), is(false));
        assertThat(reports, empty());
    }

    public void testEntrySet_iterator_remove() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Map.Entry<String, Integer>> iterator = wrapper.entrySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(0), originalValues.get(0))));
        iterator.remove();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(1), originalValues.get(1))));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(2), originalValues.get(2))));
        iterator.remove();
        assertThat(iterator.hasNext(), is(false));
        assertThat(delegate, is(Map.of(originalKeys.get(1), originalValues.get(1))));
        assertThat(reports, contains("someMap.entrySet().iterator()...remove()", "someMap.entrySet().iterator()...remove()"));
    }

    public void testEntrySet_iterator_setValueOnEntry() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Map.Entry<String, Integer>> iterator = wrapper.entrySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(0), originalValues.get(0))));
        assertThat(iterator.hasNext(), is(true));
        Map.Entry<String, Integer> entryAtIndex1 = iterator.next();
        assertThat(entryAtIndex1, is(Map.entry(originalKeys.get(1), originalValues.get(1))));
        entryAtIndex1.setValue(999);
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(2), originalValues.get(2))));
        assertThat(iterator.hasNext(), is(false));
        assertThat(
            delegate,
            is(Map.of(originalKeys.get(0), originalValues.get(0), originalKeys.get(1), 999, originalKeys.get(2), originalValues.get(2)))
        );
        assertThat(reports, contains("someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_iterator_forEachRemaining_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Map.Entry<String, Integer>> iterator = wrapper.entrySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(0), originalValues.get(0))));
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        iterator.forEachRemaining(entriesCollected::add);
        assertThat(
            entriesCollected,
            contains(Map.entry(originalKeys.get(1), originalValues.get(1)), Map.entry(originalKeys.get(2), originalValues.get(2)))
        );
        assertThat(reports, empty());
    }

    public void testEntrySet_iterator_forEachRemaining_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Iterator<Map.Entry<String, Integer>> iterator = wrapper.entrySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(Map.entry(originalKeys.get(0), originalValues.get(0))));
        iterator.forEachRemaining(entry -> entry.setValue(entry.getValue() * 10));
        assertThat(
            delegate,
            is(
                Map.of(
                    originalKeys.get(0),
                    originalValues.get(0),
                    originalKeys.get(1),
                    originalValues.get(1) * 10,
                    originalKeys.get(2),
                    originalValues.get(2) * 10
                )
            )
        );
        assertThat(reports, contains("someMap.entrySet()...setValue(V)", "someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_stream_readyOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        List<Map.Entry<String, Integer>> streamedEntries = entrySet.stream().toList();
        assertThat(streamedEntries, containsInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_stream_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        entrySet.stream().filter(e -> e.getKey().equals("two")).forEach(e -> e.setValue(999));
        assertThat(delegate, is(Map.of("one", 1, "two", 999, "three", 3)));
        assertThat(reports, contains("someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_spliterator_tryAdvance_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Spliterator<Map.Entry<String, Integer>> spliterator = entrySet.spliterator();
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(true));
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(true));
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(true));
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(false));
        assertThat(entriesCollected, containsInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3)));
        assertThat(reports, empty());
    }

    public void testEntrySet_spliterator_tryAdvance_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Spliterator<Map.Entry<String, Integer>> spliterator = entrySet.spliterator();
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(true));
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(true));
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(true));
        assertThat(spliterator.tryAdvance(entriesCollected::add), is(false));
        entriesCollected.forEach(e -> e.setValue(e.getValue() * 10));
        assertThat(entriesCollected, containsInAnyOrder(Map.entry("one", 10), Map.entry("two", 20), Map.entry("three", 30)));
        assertThat(delegate, is(Map.of("one", 10, "two", 20, "three", 30)));
        assertThat(
            reports,
            contains("someMap.entrySet()...setValue(V)", "someMap.entrySet()...setValue(V)", "someMap.entrySet()...setValue(V)")
        );
    }

    public void testEntrySet_spliterator_forEachRemaining_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Spliterator<Map.Entry<String, Integer>> spliterator = entrySet.spliterator();
        assertThat(spliterator.tryAdvance(e -> {}), is(true));
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        spliterator.forEachRemaining(entriesCollected::add);
        assertThat(
            entriesCollected,
            contains(Map.entry(originalKeys.get(1), originalValues.get(1)), Map.entry(originalKeys.get(2), originalValues.get(2)))
        );
        assertThat(reports, empty());
    }

    public void testEntrySet_spliterator_forEachRemaining_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3));
        List<String> originalKeys = List.copyOf(delegate.keySet());
        List<Integer> originalValues = List.copyOf(delegate.values());
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Spliterator<Map.Entry<String, Integer>> spliterator = entrySet.spliterator();
        assertThat(spliterator.tryAdvance(e -> {}), is(true));
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        spliterator.forEachRemaining(entriesCollected::add);
        entriesCollected.forEach(e -> e.setValue(e.getValue() * 10));
        assertThat(
            entriesCollected,
            contains(Map.entry(originalKeys.get(1), originalValues.get(1) * 10), Map.entry(originalKeys.get(2), originalValues.get(2) * 10))
        );
        assertThat(
            delegate,
            is(
                Map.of(
                    originalKeys.get(0),
                    originalValues.get(0),
                    originalKeys.get(1),
                    originalValues.get(1) * 10,
                    originalKeys.get(2),
                    originalValues.get(2) * 10
                )
            )
        );
        assertThat(reports, contains("someMap.entrySet()...setValue(V)", "someMap.entrySet()...setValue(V)"));
    }

    public void testEntrySet_spliterator_trySplit_readOnly() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3, "four", 4);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Spliterator<Map.Entry<String, Integer>> spliterator1 = entrySet.spliterator();
        Spliterator<Map.Entry<String, Integer>> spliterator2 = spliterator1.trySplit();
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        spliterator1.forEachRemaining(entriesCollected::add);
        spliterator2.forEachRemaining(entriesCollected::add);
        assertThat(
            entriesCollected,
            containsInAnyOrder(Map.entry("one", 1), Map.entry("two", 2), Map.entry("three", 3), Map.entry("four", 4))
        );
        assertThat(reports, empty());
    }

    public void testEntrySet_spliterator_trySplit_setValue() {
        Map<String, Integer> delegate = new HashMap<>(Map.of("one", 1, "two", 2, "three", 3, "four", 4));
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        Spliterator<Map.Entry<String, Integer>> spliterator1 = entrySet.spliterator();
        Spliterator<Map.Entry<String, Integer>> spliterator2 = spliterator1.trySplit();
        List<Map.Entry<String, Integer>> entriesCollected = new ArrayList<>();
        spliterator1.forEachRemaining(entriesCollected::add);
        spliterator2.forEachRemaining(entriesCollected::add);
        entriesCollected.forEach(e -> e.setValue(e.getValue() * 10));
        assertThat(
            entriesCollected,
            containsInAnyOrder(Map.entry("one", 10), Map.entry("two", 20), Map.entry("three", 30), Map.entry("four", 40))
        );
        assertThat(delegate, is(Map.of("one", 10, "two", 20, "three", 30, "four", 40)));
        assertThat(
            reports,
            contains(
                "someMap.entrySet()...setValue(V)",
                "someMap.entrySet()...setValue(V)",
                "someMap.entrySet()...setValue(V)",
                "someMap.entrySet()...setValue(V)"
            )
        );
    }

    public void testEntrySet_equals() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThat(entrySet.equals(Map.copyOf(delegate).entrySet()), is(true));
        assertThat(entrySet.equals(Map.of("one", 1, "two", 2, "three", 9999).entrySet()), is(false));
        assertThat(entrySet.equals(Map.of("one", 1, "two", 2, "something else", 3).entrySet()), is(false));
        assertThat(entrySet.equals(null), is(false));
        assertThat(reports, empty());
    }

    public void testEntrySet_hashCode() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThat(entrySet.hashCode(), is(delegate.entrySet().hashCode()));
        assertThat(reports, empty());
    }

    public void testEntrySet_toString() {
        Map<String, Integer> delegate = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        WriteReportingMap<String, Integer> wrapper = new WriteReportingMap<>(delegate, reports::add, "someMap");
        Set<Map.Entry<String, Integer>> entrySet = wrapper.entrySet();
        assertThat(entrySet.toString(), is(delegate.entrySet().toString()));
        assertThat(reports, empty());
    }
}
