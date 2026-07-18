/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class DataPathTests extends ESTestCase {

    public void testFieldThenIndex() {
        DataObject root = new DataObject().put("items", new DataArray().add("a").add("b"));
        DataPath path = new DataPath(List.of(new DataPath.Field("items"), new DataPath.Index(1)));
        assertThat(path.query(root), equalTo(Optional.of(new DataString("b"))));
    }

    public void testEmptyPathReturnsRoot() {
        DataObject root = new DataObject();
        assertThat(new DataPath(List.of()).query(root), equalTo(Optional.of(root)));
    }

    public void testMissingFieldIsEmpty() {
        DataObject root = new DataObject().with("a", "1");
        assertThat(new DataPath(List.of(new DataPath.Field("b"))).query(root), equalTo(Optional.empty()));
    }

    public void testIndexOutOfBoundsIsEmpty() {
        DataArray array = new DataArray().add("only");
        assertThat(new DataPath(List.of(new DataPath.Index(3))).query(array), equalTo(Optional.empty()));
        assertThat(new DataPath(List.of(new DataPath.Index(-1))).query(array), equalTo(Optional.empty()));
    }

    public void testShapeMismatchIsEmpty() {
        DataObject root = new DataObject().with("a", "1");
        assertThat(new DataPath(List.of(new DataPath.Index(0))).query(root), equalTo(Optional.empty()));
        assertThat(new DataPath(List.of(new DataPath.Field("a"), new DataPath.Field("b"))).query(root), equalTo(Optional.empty()));
    }
}
