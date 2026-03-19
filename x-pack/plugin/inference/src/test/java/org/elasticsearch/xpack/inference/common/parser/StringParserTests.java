/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class StringParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String PROPERTIES = "properties";
    private static final int WRONG_TYPE_ITEM = 42;

    public void testExtractStringList_ReturnsEmpty_WhenKeyMissing() {
        var map = new HashMap<String, Object>();

        var result = StringParser.extractStringList(map, PROPERTIES, ROOT);

        assertThat(result, equalTo(List.of()));
    }

    public void testExtractStringList_ReturnsEmpty_WhenValueNull() {
        var map = new HashMap<String, Object>();
        map.put(PROPERTIES, null);

        var result = StringParser.extractStringList(map, PROPERTIES, ROOT);

        assertThat(result, equalTo(List.of()));
    }

    public void testExtractStringList_ReturnsList_WhenAllStrings() {
        var propertiesList = List.of("a", "b", "c");
        var map = new HashMap<String, Object>();
        map.put(PROPERTIES, propertiesList);

        var result = StringParser.extractStringList(map, PROPERTIES, ROOT);

        assertThat(result, equalTo(propertiesList));
    }

    public void testExtractStringList_ReturnsEmptyList_WhenEmptyList() {
        var map = new HashMap<String, Object>();
        map.put(PROPERTIES, List.of());

        var result = StringParser.extractStringList(map, PROPERTIES, ROOT);

        assertThat(result, equalTo(List.of()));
    }

    public void testExtractStringList_Throws_WhenItemNotString() {
        var map = new HashMap<String, Object>();
        map.put(PROPERTIES, List.of("a", WRONG_TYPE_ITEM, "c"));

        var e = expectThrows(IllegalArgumentException.class, () -> StringParser.extractStringList(map, PROPERTIES, ROOT));
        assertThat(e.getMessage(), containsString(pathToKey(ROOT, PROPERTIES)));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_ITEM)));
        assertThat(e.getMessage(), containsString("Integer"));
    }
}
