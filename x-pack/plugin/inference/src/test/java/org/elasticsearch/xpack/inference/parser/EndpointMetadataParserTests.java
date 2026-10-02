/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.parser;

import org.elasticsearch.inference.StatusHeuristic;
import org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.test.ESTestCase;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.metadata.EndpointMetadata.CAPABILITIES_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Capabilities.CONTEXT_WINDOW_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Capabilities.REASONING_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ContextWindow.MAX_INPUT_TOKENS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ContextWindow.MAX_OUTPUT_TOKENS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.DENIED_BY_REGION_POLICY_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.DISPLAY_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Display.MODEL_CREATOR_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Display.NAME_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.CSP_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.GEO_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.REGION_DISPLAY_NAME_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.EndpointRegion.REGION_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.HEURISTICS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.END_OF_LIFE_DATE_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.PROPERTIES_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.RELEASE_DATE_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Heuristics.STATUS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.INTERNAL_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Internal.FINGERPRINT_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.Internal.VERSION_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.METADATA_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.MODEL_IDENTITY_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ModelIdentity.CREATOR_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ModelIdentity.FAMILY_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ModelIdentity.TIER_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ModelIdentity.VERSION_FIELD;
import static org.elasticsearch.inference.metadata.EndpointMetadata.REGIONS_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ReasoningCapability.DEFAULT_EFFORT_LEVEL_FIELD_NAME;
import static org.elasticsearch.inference.metadata.EndpointMetadata.ReasoningCapability.SUPPORTED_EFFORT_LEVELS_FIELD_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class EndpointMetadataParserTests extends ESTestCase {

    private static final String ROOT = "root";
    private static final String OTHER_KEY = "other";
    private static final String VALUE = "value";
    private static final String DISPLAY_NAME = "Display Name";
    private static final String DISPLAY_MODEL_CREATOR = "Display Model Creator";
    private static final String MY_ENDPOINT = "My Endpoint";
    private static final String MY_ENDPOINT_CREATOR = "My Creator";
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
        displayMap.put(MODEL_CREATOR_FIELD, MY_ENDPOINT_CREATOR);

        var modelIdentityMap = new HashMap<String, Object>();
        modelIdentityMap.put(CREATOR_FIELD, "test_ai");
        modelIdentityMap.put(FAMILY_FIELD, "test_model");
        modelIdentityMap.put(TIER_FIELD, "test_tier");
        modelIdentityMap.put(VERSION_FIELD, "4.2");

        var regionMap = new HashMap<String, Object>();
        regionMap.put(CSP_FIELD.getPreferredName(), "aws");
        regionMap.put(REGION_FIELD.getPreferredName(), "us-east-1");
        regionMap.put(GEO_FIELD.getPreferredName(), "us");

        var metadataMap = new HashMap<String, Object>();
        metadataMap.put(HEURISTICS_FIELD_NAME, heuristicsMap);
        metadataMap.put(INTERNAL_FIELD_NAME, internalMap);
        metadataMap.put(DISPLAY_FIELD_NAME, displayMap);
        metadataMap.put(MODEL_IDENTITY_FIELD_NAME, modelIdentityMap);
        metadataMap.put(REGIONS_FIELD_NAME, List.of(regionMap));
        metadataMap.put(DENIED_BY_REGION_POLICY_FIELD_NAME, true);

        var rootMap = new HashMap<String, Object>();
        rootMap.put(METADATA_FIELD_NAME, metadataMap);

        var result = EndpointMetadataParser.fromMap(rootMap);

        assertThat(result.heuristics().properties(), equalTo(PROPERTIES_LIST));
        assertThat(result.heuristics().status(), equalTo(expectedStatus));
        assertThat(result.heuristics().releaseDate(), equalTo(LocalDate.parse(releaseDate)));
        assertThat(result.heuristics().endOfLifeDate(), equalTo(LocalDate.parse(endOfLifeDate)));

        assertThat(result.internal().fingerprint(), equalTo(FINGERPRINT_FP123));
        assertThat(result.internal().version(), equalTo(version));

        assertThat(result.display(), equalTo(new EndpointMetadata.Display(MY_ENDPOINT, MY_ENDPOINT_CREATOR)));

        assertThat(result.modelIdentity(), equalTo(new EndpointMetadata.ModelIdentity("test_ai", "test_model", "test_tier", "4.2")));

        assertThat(result.regions(), equalTo(List.of(new EndpointMetadata.EndpointRegion("aws", "us-east-1", "us", null))));
        assertTrue(result.deniedByRegionPolicy());
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

    public void testDisplayFromMap_ParsesAllFields() {
        var map = new HashMap<String, Object>();
        map.put(NAME_FIELD, DISPLAY_NAME);
        map.put(MODEL_CREATOR_FIELD, DISPLAY_MODEL_CREATOR);

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, equalTo(new EndpointMetadata.Display(DISPLAY_NAME, DISPLAY_MODEL_CREATOR)));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenDisplayIsMissing() {
        var map = new HashMap<String, Object>();

        var result = EndpointMetadataParser.displayFromMap(map, ROOT);

        assertThat(result, sameInstance(EndpointMetadata.Display.EMPTY_INSTANCE));
    }

    public void testDisplayFromMap_ReturnsEmpty_WhenAllFieldsAreNull() {
        var map = new HashMap<String, Object>();
        map.put(NAME_FIELD, null);
        map.put(MODEL_CREATOR_FIELD, null);

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

    public void testDisplayFromMap_Throws_WhenModelCreatorWrongType() {
        var map = new HashMap<String, Object>();
        map.put(MODEL_CREATOR_FIELD, WRONG_TYPE_DISPLAY);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.displayFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(MODEL_CREATOR_FIELD));
        assertThat(e.getMessage(), containsString(String.valueOf(WRONG_TYPE_DISPLAY)));
        assertThat(e.getMessage(), containsString(STRING_CLASS_FAILURE));
    }

    public void testRegionsFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.regionsFromMap(null, ROOT), equalTo(List.of()));
    }

    public void testRegionsFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(EndpointMetadataParser.regionsFromMap(Map.of(), ROOT), equalTo(List.of()));
    }

    public void testRegionsFromMap_ReturnsEmpty_WhenRegionsKeyMissing() {
        var map = new HashMap<String, Object>();
        map.put(OTHER_KEY, VALUE);
        assertThat(EndpointMetadataParser.regionsFromMap(map, ROOT), equalTo(List.of()));
    }

    public void testRegionsFromMap_ParsesSingleRegion() {
        var regionMap = new HashMap<String, Object>();
        regionMap.put(CSP_FIELD.getPreferredName(), "aws");
        regionMap.put(REGION_FIELD.getPreferredName(), "us-east-1");
        regionMap.put(GEO_FIELD.getPreferredName(), "us");
        regionMap.put(REGION_DISPLAY_NAME_FIELD.getPreferredName(), "US East (N. Virginia)");

        var map = new HashMap<String, Object>();
        map.put(REGIONS_FIELD_NAME, List.of(regionMap));

        var result = EndpointMetadataParser.regionsFromMap(map, ROOT);

        assertThat(result, equalTo(List.of(new EndpointMetadata.EndpointRegion("aws", "us-east-1", "us", "US East (N. Virginia)"))));
    }

    public void testRegionsFromMap_ParsesMultipleRegions() {
        var region1 = new HashMap<String, Object>();
        region1.put(CSP_FIELD.getPreferredName(), "aws");
        region1.put(REGION_FIELD.getPreferredName(), "us-east-1");
        region1.put(GEO_FIELD.getPreferredName(), "us");

        var region2 = new HashMap<String, Object>();
        region2.put(CSP_FIELD.getPreferredName(), "gcp");
        region2.put(REGION_FIELD.getPreferredName(), "europe-west1");
        region2.put(GEO_FIELD.getPreferredName(), "eu");

        var map = new HashMap<String, Object>();
        map.put(REGIONS_FIELD_NAME, List.of(region1, region2));

        var result = EndpointMetadataParser.regionsFromMap(map, ROOT);

        assertThat(
            result,
            equalTo(
                List.of(
                    new EndpointMetadata.EndpointRegion("aws", "us-east-1", "us", null),
                    new EndpointMetadata.EndpointRegion("gcp", "europe-west1", "eu", null)
                )
            )
        );
    }

    public void testRegionsFromMap_HandlesNullFields() {
        var map = new HashMap<String, Object>();
        map.put(REGIONS_FIELD_NAME, List.of(new HashMap<String, Object>()));

        var result = EndpointMetadataParser.regionsFromMap(map, ROOT);

        assertThat(result, equalTo(List.of(new EndpointMetadata.EndpointRegion(null, null, null, null))));
    }

    public void testRegionsFromMap_Throws_WhenItemIsNotAMap() {
        var map = new HashMap<String, Object>();
        map.put(REGIONS_FIELD_NAME, List.of("not-a-map"));

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.regionsFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(REGIONS_FIELD_NAME));
    }

    public void testDeniedByRegionPolicyFromMap_ReturnsFalse_WhenMapIsNull() {
        assertFalse(EndpointMetadataParser.deniedByRegionPolicyFromMap(null, ROOT));
    }

    public void testDeniedByRegionPolicyFromMap_ReturnsFalse_WhenKeyMissing() {
        assertFalse(EndpointMetadataParser.deniedByRegionPolicyFromMap(Map.of(), ROOT));
    }

    public void testDeniedByRegionPolicyFromMap_ReturnsTrue_WhenTrue() {
        var map = new HashMap<String, Object>();
        map.put(DENIED_BY_REGION_POLICY_FIELD_NAME, true);

        assertTrue(EndpointMetadataParser.deniedByRegionPolicyFromMap(map, ROOT));
    }

    public void testDeniedByRegionPolicyFromMap_ReturnsFalse_WhenFalse() {
        var map = new HashMap<String, Object>();
        map.put(DENIED_BY_REGION_POLICY_FIELD_NAME, false);

        assertFalse(EndpointMetadataParser.deniedByRegionPolicyFromMap(map, ROOT));
    }

    public void testModelIdentityFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.modelIdentityFromMap(null, ROOT), sameInstance(EndpointMetadata.ModelIdentity.EMPTY_INSTANCE));
    }

    public void testModelIdentityFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(
            EndpointMetadataParser.modelIdentityFromMap(Map.of(), ROOT),
            sameInstance(EndpointMetadata.ModelIdentity.EMPTY_INSTANCE)
        );
    }

    public void testModelIdentityFromMap_ReturnsEmpty_WhenAllFieldsAreNull() {
        var map = new HashMap<String, Object>();
        map.put(CREATOR_FIELD, null);
        map.put(FAMILY_FIELD, null);
        map.put(TIER_FIELD, null);
        map.put(VERSION_FIELD, null);

        assertThat(EndpointMetadataParser.modelIdentityFromMap(map, ROOT), sameInstance(EndpointMetadata.ModelIdentity.EMPTY_INSTANCE));
    }

    public void testModelIdentityFromMap_ParsesAllFields() {
        var map = new HashMap<String, Object>();
        map.put(CREATOR_FIELD, "anthropic");
        map.put(FAMILY_FIELD, "claude");
        map.put(TIER_FIELD, "sonnet");
        map.put(VERSION_FIELD, "4.6");

        var result = EndpointMetadataParser.modelIdentityFromMap(map, ROOT);

        assertThat(result, equalTo(new EndpointMetadata.ModelIdentity("anthropic", "claude", "sonnet", "4.6")));
    }

    public void testModelIdentityFromMap_ParsesPartialFields() {
        var map = new HashMap<String, Object>();
        map.put(CREATOR_FIELD, "elastic");
        map.put(FAMILY_FIELD, "elser");

        var result = EndpointMetadataParser.modelIdentityFromMap(map, ROOT);

        assertThat(result, equalTo(new EndpointMetadata.ModelIdentity("elastic", "elser", null, null)));
    }

    public void testModelIdentityFromMap_Throws_WhenCreatorWrongType() {
        var map = new HashMap<String, Object>();
        map.put(CREATOR_FIELD, 999);

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.modelIdentityFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(CREATOR_FIELD));
    }

    public void testCapabilitiesFromMap_ReturnsEmpty_WhenMapIsNull() {
        assertThat(EndpointMetadataParser.capabilitiesFromMap(null, ROOT), sameInstance(EndpointMetadata.Capabilities.EMPTY_INSTANCE));
    }

    public void testCapabilitiesFromMap_ReturnsEmpty_WhenMapIsEmpty() {
        assertThat(
            EndpointMetadataParser.capabilitiesFromMap(new HashMap<>(), ROOT),
            sameInstance(EndpointMetadata.Capabilities.EMPTY_INSTANCE)
        );
    }

    public void testCapabilitiesFromMap_ReturnsFullCapabilities() {
        var reasoningMap = new HashMap<String, Object>();
        reasoningMap.put(SUPPORTED_EFFORT_LEVELS_FIELD_NAME, List.of("high", "medium", "low"));
        reasoningMap.put(DEFAULT_EFFORT_LEVEL_FIELD_NAME, "medium");

        var contextWindowMap = new HashMap<String, Object>();
        contextWindowMap.put(MAX_INPUT_TOKENS_FIELD_NAME, 1050000);
        contextWindowMap.put(MAX_OUTPUT_TOKENS_FIELD_NAME, 128000);

        var map = new HashMap<String, Object>();
        map.put(REASONING_FIELD_NAME, reasoningMap);
        map.put(CONTEXT_WINDOW_FIELD_NAME, contextWindowMap);

        var result = EndpointMetadataParser.capabilitiesFromMap(map, ROOT);

        assertThat(
            result.reasoning().supportedEffortLevels(),
            equalTo(List.of(ReasoningEffort.HIGH, ReasoningEffort.MEDIUM, ReasoningEffort.LOW))
        );
        assertThat(result.reasoning().defaultEffortLevel(), equalTo(ReasoningEffort.MEDIUM));
        assertThat(result.contextWindow().maxInputTokens(), equalTo(1050000));
        assertThat(result.contextWindow().maxOutputTokens(), equalTo(128000));
    }

    public void testCapabilitiesFromMap_ReasoningOnly() {
        var reasoningMap = new HashMap<String, Object>();
        reasoningMap.put(SUPPORTED_EFFORT_LEVELS_FIELD_NAME, List.of("high"));

        var map = new HashMap<String, Object>();
        map.put(REASONING_FIELD_NAME, reasoningMap);

        var result = EndpointMetadataParser.capabilitiesFromMap(map, ROOT);

        assertThat(result.reasoning().supportedEffortLevels(), equalTo(List.of(ReasoningEffort.HIGH)));
        assertThat(result.contextWindow(), nullValue());
    }

    public void testCapabilitiesFromMap_ContextWindowOnly() {
        var contextWindowMap = new HashMap<String, Object>();
        contextWindowMap.put(MAX_INPUT_TOKENS_FIELD_NAME, 200000);

        var map = new HashMap<String, Object>();
        map.put(CONTEXT_WINDOW_FIELD_NAME, contextWindowMap);

        var result = EndpointMetadataParser.capabilitiesFromMap(map, ROOT);

        assertThat(result.reasoning(), nullValue());
        assertThat(result.contextWindow().maxInputTokens(), equalTo(200000));
        assertThat(result.contextWindow().maxOutputTokens(), nullValue());
    }

    public void testReasoningCapabilityFromMap_FiltersUnknownEffortValues() {
        var map = new HashMap<String, Object>();
        map.put(SUPPORTED_EFFORT_LEVELS_FIELD_NAME, List.of("high", "unknown_future_value", "low"));
        map.put(DEFAULT_EFFORT_LEVEL_FIELD_NAME, "unknown_future_value");

        var result = EndpointMetadataParser.reasoningCapabilityFromMap(map, ROOT);

        assertThat(result.supportedEffortLevels(), equalTo(List.of(ReasoningEffort.HIGH, ReasoningEffort.LOW)));
        assertThat(result.defaultEffortLevel(), nullValue());
    }

    public void testContextWindowFromMap_AcceptsLongTokenCounts() {
        var map = new HashMap<String, Object>();
        map.put(MAX_INPUT_TOKENS_FIELD_NAME, 1050000L);
        map.put(MAX_OUTPUT_TOKENS_FIELD_NAME, 128000L);

        var result = EndpointMetadataParser.contextWindowFromMap(map, ROOT);

        assertThat(result.maxInputTokens(), equalTo(1050000));
        assertThat(result.maxOutputTokens(), equalTo(128000));
    }

    public void testContextWindowFromMap_Throws_WhenMaxInputTokensWrongType() {
        var map = new HashMap<String, Object>();
        map.put(MAX_INPUT_TOKENS_FIELD_NAME, "not-a-number");

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.contextWindowFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(MAX_INPUT_TOKENS_FIELD_NAME));
    }

    public void testReasoningCapabilityFromMap_Throws_WhenSupportedEffortLevelsWrongType() {
        var map = new HashMap<String, Object>();
        map.put(SUPPORTED_EFFORT_LEVELS_FIELD_NAME, "high");

        var e = expectThrows(IllegalArgumentException.class, () -> EndpointMetadataParser.reasoningCapabilityFromMap(map, ROOT));
        assertThat(e.getMessage(), containsString(SUPPORTED_EFFORT_LEVELS_FIELD_NAME));
    }

    /**
     * Sub-maps are extracted with an unchecked cast, matching the sibling {@code display} and {@code regions} parsers.
     */
    public void testCapabilitiesFromMap_Throws_WhenContextWindowWrongType() {
        var map = new HashMap<String, Object>();
        map.put(CONTEXT_WINDOW_FIELD_NAME, "not-an-object");

        expectThrows(ClassCastException.class, () -> EndpointMetadataParser.capabilitiesFromMap(map, ROOT));
    }

    public void testFromMap_ParsesCapabilitiesBlock() {
        var reasoningMap = new HashMap<String, Object>();
        reasoningMap.put(SUPPORTED_EFFORT_LEVELS_FIELD_NAME, List.of("high", "low"));
        reasoningMap.put(DEFAULT_EFFORT_LEVEL_FIELD_NAME, "high");

        var capabilitiesMap = new HashMap<String, Object>();
        capabilitiesMap.put(REASONING_FIELD_NAME, reasoningMap);

        var metadataMap = new HashMap<String, Object>();
        metadataMap.put(HEURISTICS_FIELD_NAME, new HashMap<>());
        metadataMap.put(INTERNAL_FIELD_NAME, new HashMap<>());
        metadataMap.put(DISPLAY_FIELD_NAME, new HashMap<>());
        metadataMap.put(CAPABILITIES_FIELD_NAME, capabilitiesMap);

        var map = new HashMap<String, Object>();
        map.put(METADATA_FIELD_NAME, metadataMap);

        var result = EndpointMetadataParser.fromMap(map);

        assertThat(result.capabilities().reasoning().supportedEffortLevels(), equalTo(List.of(ReasoningEffort.HIGH, ReasoningEffort.LOW)));
        assertThat(result.capabilities().reasoning().defaultEffortLevel(), equalTo(ReasoningEffort.HIGH));
    }
}
