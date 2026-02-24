/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NumberParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String VERSION = "version";
    private static final String NOT_A_NUMBER = "not_a_number";

    public void testExtractLong_HandlesInteger() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, 1);

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, equalTo(1L));
    }

    public void testExtractLong_HandlesLong() {
        var version = 1000L;
        var map = new HashMap<String, Object>();
        map.put(VERSION, version);

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, equalTo(version));
    }

    public void testExtractLong_ReturnsNull_WhenKeyMissing() {
        var map = new HashMap<String, Object>();

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, nullValue());
    }

    public void testExtractLong_ReturnsNull_WhenValueNull() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, null);

        var result = NumberParser.extractLong(map, VERSION, ROOT);

        assertThat(result, nullValue());
    }

    public void testExtractLong_Throws_WhenWrongType() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, NOT_A_NUMBER);

        var e = expectThrows(IllegalArgumentException.class, () -> NumberParser.extractLong(map, VERSION, ROOT));
        assertThat(e.getMessage(), containsString(pathToKey(ROOT, VERSION)));
        assertThat(e.getMessage(), containsString(NOT_A_NUMBER));
    }
}
