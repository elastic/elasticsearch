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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class WriteReportingSetTests extends ESTestCase {

    public void testContains() {
        Set<Integer> delegate = Set.of(1, 2, 3, 4, 5, 6);
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        assertThat(wrapper.contains(3), is(true));
        assertThat(wrapper.contains(7), is(false));
        assertThat(reports, empty());
    }

    public void testAdd() {
        Set<Integer> delegate = new HashSet<>(Set.of(1, 2, 3, 4, 5, 6));
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        assertThat(wrapper.add(3), is(false));
        assertThat(wrapper.add(7), is(true));
        assertThat(delegate, containsInAnyOrder(1, 2, 3, 4, 5, 6, 7));
        assertThat(reports, contains("someSet.add(E)", "someSet.add(E)"));
    }

    public void testRemoveIf() {
        Set<Integer> delegate = new HashSet<>(Set.of(1, 2, 3, 4, 5, 6));
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        assertThat(wrapper.removeIf(i -> i % 2 == 0), is(true));
        assertThat(delegate, containsInAnyOrder(1, 3, 5));
        assertThat(reports, contains("someSet.removeIf(Predicate<? super E>)"));
    }

    public void testIterator_readOnly() {
        Set<Integer> delegate = Set.of(1, 2, 3);
        List<Integer> originalElements = List.copyOf(delegate);
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        Iterator<Integer> iterator = wrapper.iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalElements.get(0)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalElements.get(1)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalElements.get(2)));
        assertThat(iterator.hasNext(), is(false));
        assertThat(reports, empty());
    }

    public void testIterator_remove() {
        Set<Integer> delegate = new HashSet<>(Set.of(1, 2, 3));
        List<Integer> originalElements = List.copyOf(delegate);
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        Iterator<Integer> iterator = wrapper.iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalElements.get(0)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalElements.get(1)));
        iterator.remove();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(originalElements.get(2)));
        assertThat(iterator.hasNext(), is(false));
        assertThat(delegate, contains(originalElements.get(0), originalElements.get(2)));
        assertThat(reports, contains("someSet.iterator()...remove()"));
    }

    public void testEquals() {
        Set<Integer> delegate = Set.of(1, 2, 3);
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        assertThat(wrapper.equals(Set.copyOf(delegate)), is(true));
        assertThat(wrapper.equals(Set.of(3, 2, 1)), is(true));
        assertThat(wrapper.equals(Set.of(1, 2)), is(false));
        assertThat(wrapper.equals(Set.of(1, 2, 3, 4)), is(false));
        assertThat(wrapper.equals(null), is(false));
        assertThat(reports, empty());
    }

    public void testHashCode() {
        Set<Integer> delegate = Set.of(1, 2, 3);
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        assertThat(wrapper.hashCode(), is(Set.of(3, 2, 1).hashCode()));
        assertThat(reports, empty());
    }

    public void testToString() {
        Set<Integer> delegate = Set.of(1, 2, 3);
        List<String> reports = new ArrayList<>();
        WriteReportingSet<Integer> wrapper = new WriteReportingSet<>(delegate, reports::add, "someSet");
        assertThat(wrapper.toString(), is(delegate.toString()));
        assertThat(reports, empty());
    }

    public void testConstructor_nullDelegate() {
        List<String> reports = new ArrayList<>();
        assertThrows(NullPointerException.class, () -> new WriteReportingSet<>(null, reports::add, "someSet"));
        assertThat(reports, empty());
    }

    public void testConstructor_nullReporter() {
        Set<Integer> delegate = Set.of(1, 2, 3);
        assertThrows(NullPointerException.class, () -> new WriteReportingSet<>(delegate, null, "someSet"));
    }
}
