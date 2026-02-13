/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ObjectParserUtilsTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String CONFIG = "config";
    private static final String KEY = "key";
    private static final String FIELD = "field";
    private static final String VALUE = "value";
    private static final String OTHER = "other";
    private static final int WRONG_TYPE_INT = 42;

    public void testPathToKey_ReturnsSubPath_WhenRootIsEmpty() {
        var single = "single";
        assertThat(pathToKey("", "a", "b"), equalTo("a.b"));
        assertThat(pathToKey(null, single), equalTo(single));
    }

    public void testPathToKey_ReturnsRootPlusSubPath_WhenRootNonEmpty() {
        assertThat(pathToKey(ROOT, "a", "b"), equalTo("root.a.b"));
        assertThat(pathToKey(CONFIG, FIELD), equalTo("config.field"));
    }

    public void testPathToKey_ReturnsRoot_WhenNoProperties() {
        assertThat(pathToKey(ROOT), equalTo(ROOT));
    }

    public void testPathToKey_SingleProperty() {
        assertThat(pathToKey("", KEY), equalTo(KEY));
    }

    public void testIsMapNullOrEmpty_TrueForNull() {
        assertTrue(ObjectParserUtils.isMapNullOrEmpty(null));
    }

    public void testIsMapNullOrEmpty_TrueForEmptyMap() {
        assertTrue(ObjectParserUtils.isMapNullOrEmpty(Map.of()));
    }

    public void testIsMapNullOrEmpty_FalseForNonEmptyMap() {
        assertFalse(ObjectParserUtils.isMapNullOrEmpty(Map.of("k", "v")));
    }

    public void testRemoveAsType_ReturnsValue_WhenPresentAndCorrectType() {
        var map = new HashMap<String, Object>();
        map.put(KEY, VALUE);

        var result = ObjectParserUtils.removeAsType(map, KEY, ROOT, String.class);

        assertThat(result, equalTo(VALUE));
        assertTrue(map.isEmpty());
    }

    public void testRemoveAsType_ReturnsNull_WhenKeyMissing() {
        var value = "x";
        var map = new HashMap<String, Object>();
        map.put(OTHER, value);

        var result = ObjectParserUtils.removeAsType(map, KEY, ROOT, String.class);

        assertThat(result, nullValue());
        assertThat(map.get(OTHER), equalTo(value));
    }

    public void testRemoveAsType_ReturnsNull_WhenValueIsNull() {
        var map = new HashMap<String, Object>();
        map.put(KEY, null);

        var result = ObjectParserUtils.removeAsType(map, KEY, ROOT, String.class);

        assertThat(result, nullValue());
    }

    public void testRemoveAsType_Throws_WhenWrongType() {
        var map = new HashMap<String, Object>();
        map.put(KEY, WRONG_TYPE_INT);

        var e = expectThrows(IllegalArgumentException.class, () -> ObjectParserUtils.removeAsType(map, KEY, CONFIG, String.class));
        assertThat(e.getMessage(), containsString(pathToKey(CONFIG, KEY)));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_INT)));
        assertThat(e.getMessage(), containsString("String"));
    }

    public void testInvalidTypeErrorMsg() {
        var msg = ObjectParserUtils.invalidTypeErrorMsg(FIELD, ROOT, "bad", "Long");
        assertThat(msg, containsString(pathToKey(ROOT, FIELD)));
        assertThat(msg, containsString("bad"));
        assertThat(msg, containsString("Long"));
    }
}
