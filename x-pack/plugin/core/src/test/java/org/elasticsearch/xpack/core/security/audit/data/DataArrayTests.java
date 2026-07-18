/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DataArrayTests extends ESTestCase {

    public void testOfStringCollectionPreservesOrder() {
        DataArray array = DataArray.of(List.of("a", "b", "c"));
        assertThat(array.size(), equalTo(3));
        assertThat(array.get(0), equalTo(new DataString("a")));
        assertThat(array.get(2), equalTo(new DataString("c")));
    }

    public void testTypedAddOverloads() {
        DataArray array = new DataArray().add("s").add(7L).add(true);
        assertThat(array.get(0), equalTo(new DataString("s")));
        assertThat(array.get(1), equalTo(new DataLong(7L)));
        assertThat(array.get(2), equalTo(new DataBoolean(true)));
    }

    public void testSetReplacesAndReturnsPrevious() {
        DataArray array = new DataArray().add("a").add("b");
        DataValue previous = array.set(1, new DataString("z"));
        assertThat(previous, equalTo(new DataString("b")));
        assertThat(array.get(1), equalTo(new DataString("z")));
    }

    public void testRemoveByIndexReturnsRemoved() {
        DataArray array = new DataArray().add("a").add("b");
        DataValue removed = array.remove(0);
        assertThat(removed, equalTo(new DataString("a")));
        assertThat(array.size(), equalTo(1));
        assertThat(array.get(0), equalTo(new DataString("b")));
    }

    public void testNullElementRejected() {
        expectThrows(NullPointerException.class, () -> new DataArray().add((DataValue) null));
        expectThrows(UnsupportedOperationException.class, () -> DataArray.of(List.of("a")).view().add(new DataString("x")));
    }

    public void testIterationPreservesOrder() {
        DataArray array = new DataArray().add("a").add("b");
        List<DataValue> seen = new ArrayList<>();
        array.forEach(seen::add);
        assertThat(seen, contains(new DataString("a"), new DataString("b")));
    }
}
