/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.matchers.source;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logsdb.datageneration.matchers.GenericEqualsMatcher;
import org.elasticsearch.logsdb.datageneration.matchers.MatchResult;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.logsdb.datageneration.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.logsdb.datageneration.matchers.Messages.prettyPrintCollections;

public class SourceMatcher extends GenericEqualsMatcher<List<Map<String, Object>>> {
    private final Map<String, MappingTransforms.FieldMapping> actualNormalizedMapping;
    private final Map<String, MappingTransforms.FieldMapping> expectedNormalizedMapping;

    private final Map<String, FieldSpecificMatcher> fieldSpecificMatchers;
    private final DynamicFieldMatcher dynamicFieldMatcher;

    public SourceMatcher(
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings,
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final List<Map<String, Object>> actual,
        final List<Map<String, Object>> expected,
        final boolean ignoringSort
    ) {
        super(actualMappings, actualSettings, expectedMappings, expectedSettings, actual, expected, ignoringSort);

        var actualMappingAsMap = XContentHelper.convertToMap(BytesReference.bytes(actualMappings), false, actualMappings.contentType())
            .v2();
        this.actualNormalizedMapping = MappingTransforms.normalizeMapping(actualMappingAsMap);

        var expectedMappingAsMap = XContentHelper.convertToMap(BytesReference.bytes(expectedMappings), false, actualMappings.contentType())
            .v2();
        this.expectedNormalizedMapping = MappingTransforms.normalizeMapping(expectedMappingAsMap);

        this.fieldSpecificMatchers = new HashMap<>() {
            {
                put("keyword", new FieldSpecificMatcher.KeywordMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("date", new FieldSpecificMatcher.DateMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put(
                    "long",
                    new FieldSpecificMatcher.NumberMatcher("long", actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "unsigned_long",
                    new FieldSpecificMatcher.UnsignedLongMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "integer",
                    new FieldSpecificMatcher.NumberMatcher("integer", actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "short",
                    new FieldSpecificMatcher.NumberMatcher("short", actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "byte",
                    new FieldSpecificMatcher.NumberMatcher("byte", actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "double",
                    new FieldSpecificMatcher.NumberMatcher("double", actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "float",
                    new FieldSpecificMatcher.NumberMatcher("float", actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "half_float",
                    new FieldSpecificMatcher.HalfFloatMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "scaled_float",
                    new FieldSpecificMatcher.ScaledFloatMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put(
                    "counted_keyword",
                    new FieldSpecificMatcher.CountedKeywordMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings)
                );
                put("boolean", new FieldSpecificMatcher.BooleanMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
            }
        };
        this.dynamicFieldMatcher = new DynamicFieldMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings);
    }

    @Override
    public MatchResult match() {
        if (actual.size() != expected.size()) {
            return MatchResult.noMatch(
                formatErrorMessage(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Number of documents does not match, " + prettyPrintCollections(actual, expected)
                )
            );
        }

        var sortedAndFlattenedActual = actual.stream().map(SourceTransforms::normalize).toList();
        var sortedAndFlattenedExpected = expected.stream().map(SourceTransforms::normalize).toList();

        for (int i = 0; i < sortedAndFlattenedActual.size(); i++) {
            var actual = sortedAndFlattenedActual.get(i);
            var expected = sortedAndFlattenedExpected.get(i);

            var result = compareSource(actual, expected);
            if (result.isMatch() == false) {
                var message = "Source matching failed at document id [" + i + "]. " + result.getMessage();
                return MatchResult.noMatch(message);
            }
        }

        return MatchResult.match();
    }

    private MatchResult compareSource(Map<String, List<Object>> actual, Map<String, List<Object>> expected) {
        for (var expectedFieldEntry : expected.entrySet()) {
            var name = expectedFieldEntry.getKey();

            var actualValues = actual.get(name);
            var expectedValues = expectedFieldEntry.getValue();

            var matchIncludingFieldSpecificMatchers = matchWithFieldSpecificMatcher(name, actualValues, expectedValues);
            if (matchIncludingFieldSpecificMatchers.isMatch() == false) {
                var message = "Source documents don't match for field [" + name + "]: " + matchIncludingFieldSpecificMatchers.getMessage();
                return MatchResult.noMatch(message);
            }
        }
        return MatchResult.match();
    }

    private MatchResult matchWithFieldSpecificMatcher(String fieldName, List<Object> actualValues, List<Object> expectedValues) {
        var actualFieldMapping = actualNormalizedMapping.get(fieldName);
        if (actualFieldMapping == null) {
            if (expectedNormalizedMapping.get(fieldName) != null
                // Special cases due to fields being defined in default mapping for logsdb index mode
                && fieldName.equals("@timestamp") == false
                && fieldName.equals("host.name") == false) {
                throw new IllegalStateException(
                    "Leaf field [" + fieldName + "] is present in expected mapping but absent in actual mapping"
                );
            }

            // Field is dynamically mapped
            return dynamicFieldMatcher.match(actualValues, expectedValues);
        }

        var actualFieldType = (String) actualFieldMapping.mappingParameters().get("type");
        if (actualFieldType == null) {
            throw new IllegalStateException("Field type is missing from leaf field Leaf field [" + fieldName + "] mapping parameters");
        }

        var expectedFieldMapping = expectedNormalizedMapping.get(fieldName);
        if (expectedFieldMapping == null) {
            throw new IllegalStateException("Leaf field [" + fieldName + "] is present in actual mapping but absent in expected mapping");
        } else {
            var expectedFieldType = expectedFieldMapping.mappingParameters().get("type");
            if (Objects.equals(actualFieldType, expectedFieldType) == false) {
                throw new IllegalStateException(
                    "Leaf field ["
                        + fieldName
                        + "] has type ["
                        + actualFieldType
                        + "] in actual mapping but a different type ["
                        + expectedFieldType
                        + "] in expected mapping"
                );
            }
        }

        var fieldSpecificMatcher = fieldSpecificMatchers.get(actualFieldType);
        assert fieldSpecificMatcher != null : "Missing matcher for field type [" + actualFieldType + "]";

        return fieldSpecificMatcher.match(
            actualValues,
            expectedValues,
            actualFieldMapping.mappingParameters(),
            expectedFieldMapping.mappingParameters()
        );
    }
}
