/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.parser;

import org.elasticsearch.inference.EndpointMetadata;
import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.test.ESTestCase;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.EndpointMetadata.DISPLAY;
import static org.elasticsearch.inference.EndpointMetadata.Display.NAME;
import static org.elasticsearch.inference.EndpointMetadata.HEURISTICS;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.END_OF_LIFE_DATE;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.PROPERTIES;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.RELEASE_DATE;
import static org.elasticsearch.inference.EndpointMetadata.Heuristics.STATUS;
import static org.elasticsearch.inference.EndpointMetadata.Internal.FINGERPRINT;
import static org.elasticsearch.inference.EndpointMetadata.Internal.VERSION;
import static org.elasticsearch.inference.EndpointMetadata.METADATA;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class EndpointMetadataParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String CONFIG = "config";
    private static final String EMPTY_ROOT = "";
    private static final String OTHER_KEY = "other";
    private static final String VALUE = "value";
    private static final String DISPLAY_NAME = "Display Name";
    private static final String MY_ENDPOINT = "My Endpoint";
    private static final String INVALID_STATUS = "invalid_status";
    private static final String NOT_A_DATE = "not-a-date";
    private static final String FINGERPRINT_ABC = "abc";
    private static final String FINGERPRINT_FP123 = "fp123";
    private static final int WRONG_TYPE_VERSION = 123;
    private static final int WRONG_TYPE_DISPLAY = 999;
    private static final String STRING_CLASS_FAILURE = "String";
    private static final List<String> PROPERTIES_LIST = List.of("prop1", "prop2");

    public void testFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.fromMap(null, EMPTY_ROOT), equalTo(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.fromMap(Map.of(), EMPTY_ROOT), equalTo(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMetadataKeyMissing() {
        var map = new HashMap<String, Object>();
        map.put(OTHER_KEY, VALUE);
        assertThat(EndpointMetadataParser.fromMap(map, EMPTY_ROOT), equalTo(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMetadataValueIsNull() {
        Map<String, Object> map = new HashMap<>();
        map.put(METADATA, null);
        assertThat(EndpointMetadataParser.fromMap(map, EMPTY_ROOT), equalTo(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMetadataValueIsEmptyMap() {
        var map = new HashMap<String, Object>();
        map.put(METADATA, new HashMap<String, Object>());
        assertThat(EndpointMetadataParser.fromMap(map, ROOT), equalTo(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ParsesFullMetadata() {
        var releaseDate = "2024-01-15";
        var endOfLifeDate = "2026-06-30";

        var heuristicsMap = new HashMap<String, Object>();
        heuristicsMap.put(PROPERTIES, PROPERTIES_LIST);
        heuristicsMap.put(STATUS, "ga");
        heuristicsMap.put(RELEASE_DATE, releaseDate);
        heuristicsMap.put(END_OF_LIFE_DATE, endOfLifeDate);

        var internalMap = new HashMap<String, Object>();
        internalMap.put(FINGERPRINT, FINGERPRINT_FP123);
        internalMap.put(VERSION, 2L);

        var displayMap = new HashMap<String, Object>();
        displayMap.put(NAME, MY_ENDPOINT);

        var metadataMap = new HashMap<String, Object>();
        metadataMap.put(HEURISTICS, heuristicsMap);
        metadataMap.put(org.elasticsearch.inference.EndpointMetadata.INTERNAL, internalMap);
        metadataMap.put(DISPLAY, displayMap);

        var rootMap = new HashMap<String, Object>();
        rootMap.put(METADATA, metadataMap);

        var result = EndpointMetadataParser.fromMap(rootMap, CONFIG);

        assertThat(result.heuristics().properties(), equalTo(PROPERTIES_LIST));
        assertThat(result.heuristics().status(), equalTo(StatusHeuristic.GA));
        assertThat(result.heuristics().releaseDate(), equalTo(LocalDate.parse(releaseDate)));
        assertThat(result.heuristics().endOfLifeDate(), equalTo(LocalDate.parse(endOfLifeDate)));

        assertThat(result.internal().fingerprint(), equalTo(FINGERPRINT_FP123));
        assertThat(result.internal().version(), equalTo(2L));

        assertThat(result.display(), equalTo(new EndpointMetadata.Display(MY_ENDPOINT)));
    }

    public void testFromMap_ParsesPartialMetadata() {
        var metadataMap = new HashMap<String, Object>();
        metadataMap.put(HEURISTICS, Map.<String, Object>of());

        var rootMap = new HashMap<String, Object>();
        rootMap.put(METADATA, metadataMap);

        var result = EndpointMetadataParser.fromMap(rootMap, EMPTY_ROOT);

        assertThat(result.heuristics(), equalTo(EndpointMetadata.Heuristics.EMPTY_INSTANCE));
        assertThat(result.internal(), equalTo(EndpointMetadata.Internal.EMPTY_INSTANCE));
        assertThat(result.display(), equalTo(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testHeuristicsFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.heuristicsFromMap(null, ROOT), equalTo(EndpointMetadata.Heuristics.EMPTY_INSTANCE));
    }

    public void testHeuristicsFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.heuristicsFromMap(Map.of(), ROOT), equalTo(EndpointMetadata.Heuristics.EMPTY_INSTANCE));
    }

    public void testHeuristicsFromMap_ParsesAllFields() {
        var releaseDate = "2025-02-01";
        var endOfLifeDate = "2027-12-31";

        var map = new HashMap<String, Object>();
        map.put(PROPERTIES, PROPERTIES_LIST);
        map.put(STATUS, "beta");
        map.put(RELEASE_DATE, releaseDate);
        map.put(END_OF_LIFE_DATE, endOfLifeDate);

        var result = EndpointMetadataParser.heuristicsFromMap(map, ROOT);

        assertThat(result.properties(), equalTo(PROPERTIES_LIST));
        assertThat(result.status(), equalTo(StatusHeuristic.BETA));
        assertThat(result.releaseDate(), equalTo(LocalDate.parse(releaseDate)));
        assertThat(result.endOfLifeDate(), equalTo(LocalDate.parse(endOfLifeDate)));
    }

    public void testHeuristicsFromMap_Throws_WhenStatusInvalid() {
        var map = new HashMap<String, Object>();
        map.put(STATUS, INVALID_STATUS);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.heuristicsFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(ROOT));
        assertThat(e.getMessage(), containsString(STATUS));
        assertThat(e.getMessage(), containsString(INVALID_STATUS));
    }

    public void testHeuristicsFromMap_Throws_WhenReleaseDateInvalid() {
        var map = new HashMap<String, Object>();
        map.put(RELEASE_DATE, NOT_A_DATE);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.heuristicsFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(RELEASE_DATE));
        assertThat(e.getMessage(), containsString(NOT_A_DATE));
        assertThat(e.getMessage(), containsString("Failed to parse"));
    }

    public void testInternalFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.internalFromMap(null, ROOT), equalTo(EndpointMetadata.Internal.EMPTY_INSTANCE));
    }

    public void testInternalFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.internalFromMap(Map.of(), ROOT), equalTo(EndpointMetadata.Internal.EMPTY_INSTANCE));
    }

    public void testInternalFromMap_ParsesFingerprintAndVersion() {
        var map = new HashMap<String, Object>();
        map.put(FINGERPRINT, FINGERPRINT_ABC);
        map.put(VERSION, 42);

        var result = EndpointMetadataParser.internalFromMap(map, ROOT);

        assertThat(result.fingerprint(), equalTo(FINGERPRINT_ABC));
        assertThat(result.version(), equalTo(42L));
    }

    public void testInternalFromMap_AcceptsIntegerVersion() {
        var map = new HashMap<String, Object>();
        map.put(VERSION, 1);

        var result = EndpointMetadataParser.internalFromMap(map, ROOT);

        assertThat(result.version(), equalTo(1L));
    }

    public void testInternalFromMap_Throws_WhenFingerprintWrongType() {
        var map = new HashMap<String, Object>();
        map.put(FINGERPRINT, WRONG_TYPE_VERSION);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.internalFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(FINGERPRINT));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_VERSION)));
        assertThat(e.getMessage(), containsString(STRING_CLASS_FAILURE));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.displayFromMap(null, ROOT), equalTo(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.displayFromMap(Map.of(), ROOT), equalTo(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_ParsesName() {
        var map = new HashMap<String, Object>();
        map.put(NAME, DISPLAY_NAME);

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, equalTo(new EndpointMetadata.Display(DISPLAY_NAME)));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenNameIsNull() {
        var map = new HashMap<String, Object>();

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, equalTo(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_Throws_WhenNameWrongType() {
        var map = new HashMap<String, Object>();
        map.put(NAME, WRONG_TYPE_DISPLAY);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.displayFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(NAME));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_DISPLAY)));
        assertThat(e.getMessage(), containsString(STRING_CLASS_FAILURE));
    }
}
