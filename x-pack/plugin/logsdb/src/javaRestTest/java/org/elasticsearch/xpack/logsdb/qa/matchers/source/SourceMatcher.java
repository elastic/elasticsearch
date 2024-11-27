/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.matchers.source;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.logsdb.qa.matchers.GenericEqualsMatcher;
import org.elasticsearch.xpack.logsdb.qa.matchers.ListEqualMatcher;
import org.elasticsearch.xpack.logsdb.qa.matchers.MatchResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.logsdb.qa.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.xpack.logsdb.qa.matchers.Messages.prettyPrintCollections;

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

        this.fieldSpecificMatchers = Map.of(
            "half_float",
            new FieldSpecificMatcher.HalfFloatMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings),
            "scaled_float",
            new FieldSpecificMatcher.ScaledFloatMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings),
            "unsigned_long",
            new FieldSpecificMatcher.UnsignedLongMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings)
        );
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

            // There are cases when field values are stored in ignored source
            // so we try to match them as is first and then apply field specific matcher.
            // This is temporary, we should be able to tell when source is exact using mappings.
            // See #111916.
            var genericMatchResult = matchWithGenericMatcher(actualValues, expectedValues);
            if (genericMatchResult.isMatch()) {
                return genericMatchResult;
            }

            var matchIncludingFieldSpecificMatchers = matchWithFieldSpecificMatcher(name, actualValues, expectedValues).orElse(
                genericMatchResult
            );
            if (matchIncludingFieldSpecificMatchers.isMatch() == false) {
                var message = "Source documents don't match for field [" + name + "]: " + matchIncludingFieldSpecificMatchers.getMessage();
                return MatchResult.noMatch(message);
            }
        }

        return MatchResult.match();
    }

    private Optional<MatchResult> matchWithFieldSpecificMatcher(String fieldName, List<Object> actualValues, List<Object> expectedValues) {
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

        if (sourceMatchesExactly(expectedFieldMapping, expectedValues)) {
            return Optional.empty();
        }

        var fieldSpecificMatcher = fieldSpecificMatchers.get(actualFieldType);
        if (fieldSpecificMatcher == null) {
            return Optional.empty();
        }

        MatchResult matched = fieldSpecificMatcher.match(
            actualValues,
            expectedValues,
            actualFieldMapping.mappingParameters(),
            expectedFieldMapping.mappingParameters()
        );
        return Optional.of(matched);
    }

    // Checks for scenarios when source is stored exactly and therefore can be compared without special logic.
    private boolean sourceMatchesExactly(MappingTransforms.FieldMapping mapping, List<Object> expectedValues) {
        return mapping.parentMappingParameters().stream().anyMatch(m -> m.getOrDefault("enabled", "true").equals("false"))
            || mapping.mappingParameters().getOrDefault("synthetic_source_keep", "none").equals("all")
            || expectedValues.size() > 1 && mapping.mappingParameters().getOrDefault("synthetic_source_keep", "none").equals("arrays");
    }

    private MatchResult matchWithGenericMatcher(List<Object> actualValues, List<Object> expectedValues) {
        var genericListMatcher = new ListEqualMatcher(
            actualMappings,
            actualSettings,
            expectedMappings,
            expectedSettings,
            SourceTransforms.normalizeValues(actualValues),
            SourceTransforms.normalizeValues(expectedValues),
            true
        );

        return genericListMatcher.match();
    }
}
