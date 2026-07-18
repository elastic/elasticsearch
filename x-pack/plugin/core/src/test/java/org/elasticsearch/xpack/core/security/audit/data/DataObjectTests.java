/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import org.elasticsearch.test.ESTestCase;

import java.util.NoSuchElementException;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataObjectTests extends ESTestCase {

    public void testGetStringAbsentOrNull() {
        DataObject object = new DataObject().with("a", "1");
        assertThat(object.getString("a"), equalTo("1"));
        assertThat(object.getString("missing"), nullValue());
        object.put("n", DataNull.INSTANCE);
        assertThat(object.getString("n"), nullValue());
    }

    public void testGetStringOnNonStringThrows() {
        DataObject object = new DataObject().put("b", true);
        expectThrows(IllegalStateException.class, () -> object.getString("b"));
    }

    public void testRequireMissingThrows() {
        expectThrows(NoSuchElementException.class, () -> new DataObject().require("x"));
    }

    public void testPutOverwritesAndRemove() {
        DataObject object = new DataObject().with("a", "1");
        object.put("a", "2");
        assertThat(object.getString("a"), equalTo("2"));
        assertThat(object.remove("a"), equalTo(Optional.of(new DataString("2"))));
        assertThat(object.get("a"), equalTo(Optional.empty()));
    }

    public void testPutLongAndBoolean() {
        DataObject object = new DataObject().put("i", 42L).put("b", true);
        assertThat(object.require("i"), equalTo(new DataLong(42L)));
        assertThat(object.require("b"), equalTo(new DataBoolean(true)));
    }
}
