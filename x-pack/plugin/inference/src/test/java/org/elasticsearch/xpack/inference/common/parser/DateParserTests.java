/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.test.ESTestCase;

import java.time.LocalDate;
import java.util.HashMap;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DateParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String CONFIG = "config";
    private static final String RELEASE_DATE_KEY = "release_date";
    private static final String KEY = "key";
    private static final String NOT_A_DATE = "not-a-date";
    private static final String INVALID_DATE_FORMAT = "01/01/2025";
    private static final String VALID_DATE = "2025-01-01";
    private static final LocalDate EXPECTED_DATE = LocalDate.of(2025, 1, 1);

    public void testParseLocalDate_FromMap_ValidDate() {
        var map = new HashMap<String, Object>();
        map.put(RELEASE_DATE_KEY, VALID_DATE);

        var result = DateParser.parseLocalDate(map, RELEASE_DATE_KEY, ROOT);

        assertThat(result, equalTo(EXPECTED_DATE));
    }

    public void testParseLocalDate_FromMap_ReturnsNull_WhenKeyMissing() {
        var map = new HashMap<String, Object>();

        var result = DateParser.parseLocalDate(map, RELEASE_DATE_KEY, ROOT);

        assertThat(result, nullValue());
    }

    public void testParseLocalDate_FromMap_ReturnsNull_WhenValueNull() {
        var map = new HashMap<String, Object>();
        map.put(RELEASE_DATE_KEY, null);

        var result = DateParser.parseLocalDate(map, RELEASE_DATE_KEY, ROOT);

        assertThat(result, nullValue());
    }

    public void testParseLocalDate_FromMap_Throws_WhenInvalidDate() {
        var map = new HashMap<String, Object>();
        map.put(RELEASE_DATE_KEY, NOT_A_DATE);

        var e = expectThrows(IllegalArgumentException.class, () -> DateParser.parseLocalDate(map, RELEASE_DATE_KEY, CONFIG));
        assertThat(e.getMessage(), containsString(pathToKey(CONFIG, RELEASE_DATE_KEY)));
        assertThat(e.getMessage(), containsString(NOT_A_DATE));
    }

    public void testParseLocalDate_FromString_ValidDate() {
        assertThat(DateParser.parseLocalDate(VALID_DATE, KEY, ROOT), equalTo(EXPECTED_DATE));
    }

    public void testParseLocalDate_FromString_ReturnsNull_WhenNull() {
        assertThat(DateParser.parseLocalDate((String) null, KEY, ROOT), nullValue());
    }

    public void testParseLocalDate_FromString_Throws_WhenInvalidFormat() {
        var e = expectThrows(IllegalArgumentException.class, () -> DateParser.parseLocalDate(INVALID_DATE_FORMAT, KEY, ROOT));
        assertThat(e.getMessage(), containsString(pathToKey(ROOT, KEY)));
        assertThat(e.getMessage(), containsString(INVALID_DATE_FORMAT));
    }
}
