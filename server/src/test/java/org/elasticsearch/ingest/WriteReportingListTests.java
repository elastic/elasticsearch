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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class WriteReportingListTests extends ESTestCase {

    public void testGet() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        assertThat(wrapper.get(1), is("bar"));
        assertThat(reports, empty());
    }

    public void testSet() {
        List<String> delegate = new ArrayList<>(List.of("foo", "bar", "baz"));
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        assertThat(wrapper.set(1, "newBar"), is("bar"));
        assertThat(reports, contains("someList.set(int, E)"));
    }

    public void testIterator_readOnly() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        Iterator<String> iterator = wrapper.iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is("foo"));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is("bar"));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is("baz"));
        assertThat(iterator.hasNext(), is(false));
        assertThat(wrapper, contains("foo", "bar", "baz"));
        assertThat(reports, empty());
    }

    public void testIterator_remove() {
        List<String> delegate = new ArrayList<>(List.of("foo", "bar", "baz"));
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        Iterator<String> iterator = wrapper.iterator();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is("foo"));
        iterator.remove();
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is("bar"));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is("baz"));
        iterator.remove();
        assertThat(iterator.hasNext(), is(false));
        assertThat(wrapper, contains("bar"));
        assertThat(reports, contains("someList.iterator()...remove()", "someList.iterator()...remove()"));
    }

    public void testListIterator_readOnly() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        ListIterator<String> listIterator = wrapper.listIterator(1);
        assertThat(listIterator.hasNext(), is(true));
        assertThat(listIterator.hasPrevious(), is(true));
        assertThat(listIterator.previous(), is("foo"));
        assertThat(listIterator.hasNext(), is(true));
        assertThat(listIterator.hasPrevious(), is(false));
        assertThat(delegate, contains("foo", "bar", "baz"));
        assertThat(reports, empty());
    }

    public void testListIterator_add() {
        List<String> delegate = new ArrayList<>(List.of("foo", "bar", "baz"));
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        ListIterator<String> listIterator = wrapper.listIterator(1);
        listIterator.add("extra");
        assertThat(delegate, contains("foo", "extra", "bar", "baz"));
        assertThat(reports, contains("someList.listIterator()...add(E)"));
    }

    public void testSubList_readOnly() {
        List<String> delegate = List.of("before", "foo", "bar", "baz", "after");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        List<String> subList = wrapper.subList(1, 4);
        assertThat(subList, contains("foo", "bar", "baz"));
        assertThat(reports, empty());
    }

    public void testSubList_clear() {
        List<String> delegate = new ArrayList<>(List.of("before", "foo", "bar", "baz", "after"));
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        List<String> subList = wrapper.subList(1, 4);
        subList.clear();
        assertThat(subList, empty());
        assertThat(delegate, contains("before", "after"));
        assertThat(reports, contains("someList.subList(int, int).clear()"));
    }

    public void testReversed_readOnly() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        List<String> reversed = wrapper.reversed();
        assertThat(reversed, contains("baz", "bar", "foo"));
        assertThat(reports, empty());
    }

    public void testReversed_addLast() {
        List<String> delegate = new ArrayList<>(List.of("foo", "bar", "baz"));
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        List<String> reversed = wrapper.reversed();
        reversed.addLast("the first and last");
        assertThat(reversed, contains("baz", "bar", "foo", "the first and last"));
        assertThat(delegate, contains("the first and last", "foo", "bar", "baz"));
        assertThat(reports, contains("someList.reversed().addLast(E)"));
    }

    public void testEquals() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        assertThat(wrapper.equals(List.copyOf(delegate)), is(true));
        assertThat(wrapper.equals(List.of("foo", "bar", "qux")), is(false));
        assertThat(wrapper.equals(null), is(false));
        assertThat(reports, empty());
    }

    public void testHashCode() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        assertThat(wrapper.hashCode(), is(delegate.hashCode()));
        assertThat(reports, empty());
    }

    public void testToString() {
        List<String> delegate = List.of("foo", "bar", "baz");
        List<String> reports = new ArrayList<>();
        WriteReportingList<String> wrapper = new WriteReportingList<>(delegate, reports::add, "someList");
        assertThat(wrapper.toString(), is(delegate.toString()));
        assertThat(reports, empty());
    }

    public void testConstructor_nullDelegate() {
        List<String> reports = new ArrayList<>();
        assertThrows(NullPointerException.class, () -> new WriteReportingList<>(null, reports::add, "someList"));
        assertThat(reports, empty());
    }

    public void testConstructor_nullReporter() {
        List<String> delegate = List.of("foo", "bar", "baz");
        assertThrows(NullPointerException.class, () -> new WriteReportingList<>(delegate, null, "someList"));
    }
}
