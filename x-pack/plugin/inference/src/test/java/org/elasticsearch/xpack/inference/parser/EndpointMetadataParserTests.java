/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.parser;

import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.test.ESTestCase;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.metadata.EndpointMetadata.DISPLAY_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Display.NAME_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.HEURISTICS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.END_OF_LIFE_DATE_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.PROPERTIES_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.RELEASE_DATE_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.STATUS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.INTERNAL_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Internal.FINGERPRINT_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Internal.VERSION_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.METADATA_FIELD_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class EndpointMetadataParserTests extends ESTestCase {

    private static final String ROOT = "root";
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
        assertThat(EndpointMetadataParser.fromMap(null), sameInstance(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.fromMap(Map.of()), sameInstance(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMetadataKeyMissing() {
        var map = new HashMap<String, Object>();
        map.put(OTHER_KEY, VALUE);
        assertThat(EndpointMetadataParser.fromMap(map), sameInstance(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMetadataValueIsNull() {
        Map<String, Object> map = new HashMap<>();
        map.put(METADATA_FIELD_NAME, null);
        assertThat(EndpointMetadataParser.fromMap(map), sameInstance(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ReturnsEmpty_WhenMetadataValueIsEmptyMap() {
        var map = new HashMap<String, Object>();
        map.put(METADATA_FIELD_NAME, new HashMap<String, Object>());
        assertThat(EndpointMetadataParser.fromMap(map), sameInstance(EndpointMetadata.EMPTY_INSTANCE));
    }

    public void testFromMap_ParsesFullMetadata() {
        var releaseDate = "2024-01-15";
        var endOfLifeDate = "2026-06-30";
        var expectedStatus = StatusHeuristic.GA;
        var version = 2L;

        var heuristicsMap = new HashMap<String, Object>();
        heuristicsMap.put(PROPERTIES_FIELD_NAME, PROPERTIES_LIST);
        heuristicsMap.put(STATUS_FIELD_NAME, expectedStatus.toString());
        heuristicsMap.put(RELEASE_DATE_FIELD_NAME, releaseDate);
        heuristicsMap.put(END_OF_LIFE_DATE_FIELD_NAME, endOfLifeDate);

        var internalMap = new HashMap<String, Object>();
        internalMap.put(FINGERPRINT_FIELD_NAME, FINGERPRINT_FP123);
        internalMap.put(VERSION_FIELD_NAME, version);

        var displayMap = new HashMap<String, Object>();
        displayMap.put(NAME_FIELD, MY_ENDPOINT);

        var metadataMap = new HashMap<String, Object>();
        metadataMap.put(HEURISTICS_FIELD_NAME, heuristicsMap);
        metadataMap.put(INTERNAL_FIELD_NAME, internalMap);
        metadataMap.put(DISPLAY_FIELD_NAME, displayMap);

        var rootMap = new HashMap<String, Object>();
        rootMap.put(METADATA_FIELD_NAME, metadataMap);

        var result = EndpointMetadataParser.fromMap(rootMap);

        assertThat(result.heuristics().properties(), equalTo(PROPERTIES_LIST));
        assertThat(result.heuristics().status(), equalTo(expectedStatus));
        assertThat(result.heuristics().releaseDate(), equalTo(LocalDate.parse(releaseDate)));
        assertThat(result.heuristics().endOfLifeDate(), equalTo(LocalDate.parse(endOfLifeDate)));

        assertThat(result.internal().fingerprint(), equalTo(FINGERPRINT_FP123));
        assertThat(result.internal().version(), equalTo(version));

        assertThat(result.display(), equalTo(new EndpointMetadata.Display(MY_ENDPOINT)));
    }

    public void testFromMap_ParsesPartialMetadata() {
        var metadataMap = new HashMap<String, Object>();
        metadataMap.put(HEURISTICS_FIELD_NAME, Map.<String, Object>of());

        var rootMap = new HashMap<String, Object>();
        rootMap.put(METADATA_FIELD_NAME, metadataMap);

        var result = EndpointMetadataParser.fromMap(rootMap);

        assertThat(result.heuristics(), sameInstance(EndpointMetadata.Heuristics.EMPTY_INSTANCE));
        assertThat(result.internal(), sameInstance(EndpointMetadata.Internal.EMPTY_INSTANCE));
        assertThat(result.display(), sameInstance(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testHeuristicsFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.heuristicsFromMap(null, ROOT), sameInstance(EndpointMetadata.Heuristics.EMPTY_INSTANCE));
    }

    public void testHeuristicsFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.heuristicsFromMap(Map.of(), ROOT), sameInstance(EndpointMetadata.Heuristics.EMPTY_INSTANCE));
    }

    public void testHeuristicsFromMap_ParsesAllFields() {
        var releaseDate = "2025-02-01";
        var endOfLifeDate = "2027-12-31";
        var expectedStatus = StatusHeuristic.BETA;

        var map = new HashMap<String, Object>();
        map.put(PROPERTIES_FIELD_NAME, PROPERTIES_LIST);
        map.put(STATUS_FIELD_NAME, expectedStatus.toString());
        map.put(RELEASE_DATE_FIELD_NAME, releaseDate);
        map.put(END_OF_LIFE_DATE_FIELD_NAME, endOfLifeDate);

        var result = EndpointMetadataParser.heuristicsFromMap(map, ROOT);

        assertThat(result.properties(), equalTo(PROPERTIES_LIST));
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.releaseDate(), equalTo(LocalDate.parse(releaseDate)));
        assertThat(result.endOfLifeDate(), equalTo(LocalDate.parse(endOfLifeDate)));
    }

    public void testHeuristicsFromMap_Throws_WhenStatusInvalid() {
        var map = new HashMap<String, Object>();
        map.put(STATUS_FIELD_NAME, INVALID_STATUS);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.heuristicsFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(ROOT));
        assertThat(e.getMessage(), containsString(STATUS_FIELD_NAME));
        assertThat(e.getMessage(), containsString(INVALID_STATUS));
    }

    public void testHeuristicsFromMap_Throws_WhenReleaseDateInvalid() {
        var map = new HashMap<String, Object>();
        map.put(RELEASE_DATE_FIELD_NAME, NOT_A_DATE);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.heuristicsFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(RELEASE_DATE_FIELD_NAME));
        assertThat(e.getMessage(), containsString(NOT_A_DATE));
        assertThat(e.getMessage(), containsString("Failed to parse"));
    }

    public void testInternalFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.internalFromMap(null, ROOT), sameInstance(EndpointMetadata.Internal.EMPTY_INSTANCE));
    }

    public void testInternalFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.internalFromMap(Map.of(), ROOT), sameInstance(EndpointMetadata.Internal.EMPTY_INSTANCE));
    }

    public void testInternalFromMap_ParsesFingerprintAndVersion() {
        var map = new HashMap<String, Object>();
        map.put(FINGERPRINT_FIELD_NAME, FINGERPRINT_ABC);
        map.put(VERSION_FIELD_NAME, 42);

        var result = EndpointMetadataParser.internalFromMap(map, ROOT);

        assertThat(result.fingerprint(), equalTo(FINGERPRINT_ABC));
        assertThat(result.version(), equalTo(42L));
    }

    public void testInternalFromMap_AcceptsIntegerVersion() {
        var map = new HashMap<String, Object>();
        map.put(VERSION_FIELD_NAME, 1);

        var result = EndpointMetadataParser.internalFromMap(map, ROOT);

        assertThat(result.version(), equalTo(1L));
    }

    public void testInternalFromMap_Throws_WhenFingerprintWrongType() {
        var map = new HashMap<String, Object>();
        map.put(FINGERPRINT_FIELD_NAME, WRONG_TYPE_VERSION);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.internalFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(FINGERPRINT_FIELD_NAME));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_VERSION)));
        assertThat(e.getMessage(), containsString(STRING_CLASS_FAILURE));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.displayFromMap(null, ROOT), sameInstance(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.displayFromMap(Map.of(), ROOT), sameInstance(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_ParsesName() {
        var map = new HashMap<String, Object>();
        map.put(NAME_FIELD, DISPLAY_NAME);

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, equalTo(new EndpointMetadata.Display(DISPLAY_NAME)));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenNameIsMissing() {
        var map = new HashMap<String, Object>();

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, sameInstance(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenNameIsNull() {
        var map = new HashMap<String, Object>();
        map.put(NAME_FIELD, null);

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, sameInstance(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_Throws_WhenNameWrongType() {
        var map = new HashMap<String, Object>();
        map.put(NAME_FIELD, WRONG_TYPE_DISPLAY);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.displayFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(NAME_FIELD));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_DISPLAY)));
        assertThat(e.getMessage(), containsString(STRING_CLASS_FAILURE));
    }
}
