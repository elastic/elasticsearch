/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.HashMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnumParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String STATUS = "status";
    private static final String GA = "ga";
    private static final String INVALID_VALUE = "invalid";

    public void testExtractEnum_ReturnsValue_WhenValid() {
        var map = new HashMap<String, Object>();
        map.put(STATUS, GA);

        var result = EnumParser.extractEnum(map, STATUS, ROOT, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class));

        assertThat(result, equalTo(StatusHeuristic.GA));
    }

    public void testExtractEnum_ReturnsNull_WhenKeyMissing() {
        var map = new HashMap<String, Object>();

        var result = EnumParser.extractEnum(map, STATUS, ROOT, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class));

        assertThat(result, nullValue());
    }

    public void testExtractEnum_ReturnsNull_WhenValueNull() {
        var map = new HashMap<String, Object>();
        map.put(STATUS, null);

        var result = EnumParser.extractEnum(map, STATUS, ROOT, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class));

        assertThat(result, nullValue());
    }

    public void testExtractEnum_Throws_WhenInvalidValue() {
        var map = new HashMap<String, Object>();
        map.put(STATUS, INVALID_VALUE);

        var e = expectThrows(
            IllegalArgumentException.class,
            () -> EnumParser.extractEnum(map, STATUS, ROOT, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class))
        );
        assertThat(e.getMessage(), containsString(ROOT));
        assertThat(e.getMessage(), containsString(STATUS));
        assertThat(e.getMessage(), containsString(INVALID_VALUE));
    }

    public void testExtractEnum_Throws_WhenValueNotInValidSet() {
        var map = new HashMap<String, Object>();
        map.put(STATUS, GA);

        var e = expectThrows(
            IllegalArgumentException.class,
            () -> EnumParser.extractEnum(
                map,
                STATUS,
                ROOT,
                StatusHeuristic::fromString,
                EnumSet.of(StatusHeuristic.BETA, StatusHeuristic.PREVIEW)
            )
        );
        assertThat(e.getMessage(), containsString(ROOT));
        assertThat(e.getMessage(), containsString(STATUS));
        assertThat(e.getMessage(), containsString(GA));
    }

    public void testExtractEnum_AcceptsCaseInsensitive() {
        var map = new HashMap<String, Object>();
        map.put(STATUS, "BETA");

        var result = EnumParser.extractEnum(map, STATUS, ROOT, StatusHeuristic::fromString, EnumSet.allOf(StatusHeuristic.class));

        assertThat(result, equalTo(StatusHeuristic.BETA));
    }
}
