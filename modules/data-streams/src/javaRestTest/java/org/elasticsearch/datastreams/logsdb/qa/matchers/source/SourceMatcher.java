/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers.source;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datastreams.logsdb.qa.matchers.GenericEqualsMatcher;
import org.elasticsearch.datastreams.logsdb.qa.matchers.ListEqualMatcher;
import org.elasticsearch.datastreams.logsdb.qa.matchers.MatchResult;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.datastreams.logsdb.qa.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datastreams.logsdb.qa.matchers.Messages.prettyPrintCollections;

public class SourceMatcher extends GenericEqualsMatcher<List<Map<String, Object>>> {
    private final Map<String, Map<String, Object>> actualNormalizedMapping;
    private final Map<String, Map<String, Object>> expectedNormalizedMapping;

    private final Map<String, FieldSpecificMatcher> fieldSpecificMatchers;

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

        var sortedAndFlattenedActual = actual.stream()
            .sorted(Comparator.comparing((Map<String, Object> m) -> parseTimestampToEpochMillis(m.get("@timestamp"))))
            .map(SourceTransforms::normalize)
            .toList();
        var sortedAndFlattenedExpected = expected.stream()
            .sorted(Comparator.comparing((Map<String, Object> m) -> parseTimestampToEpochMillis(m.get("@timestamp"))))
            .map(SourceTransforms::normalize)
            .toList();

        for (int i = 0; i < sortedAndFlattenedActual.size(); i++) {
            var actual = sortedAndFlattenedActual.get(i);
            var expected = sortedAndFlattenedExpected.get(i);

            var result = compareSource(actual, expected);
            if (result.isMatch() == false) {
                return result;
            }
        }

        return MatchResult.match();
    }

    private MatchResult compareSource(Map<String, List<Object>> actual, Map<String, List<Object>> expected) {
        for (var expectedFieldEntry : expected.entrySet()) {
            var name = expectedFieldEntry.getKey();

            var actualValues = actual.get(name);
            var expectedValues = expectedFieldEntry.getValue();

            MatchResult fieldMatch = matchWithFieldSpecificMatcher(name, actualValues, expectedValues).orElseGet(
                () -> matchWithGenericMatcher(actualValues, expectedValues)
            );

            if (fieldMatch.isMatch() == false) {
                var message = "Source documents don't match for field [" + name + "]: " + fieldMatch.getMessage();
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

            // Dynamic mapping, nothing to do
            return Optional.empty();
        }

        var actualFieldType = (String) actualFieldMapping.get("type");
        if (actualFieldType == null) {
            throw new IllegalStateException("Field type is missing from leaf field Leaf field [" + fieldName + "] mapping parameters");
        }

        var expectedFieldMapping = expectedNormalizedMapping.get(fieldName);
        if (expectedFieldMapping == null) {
            throw new IllegalStateException("Leaf field [" + fieldName + "] is present in actual mapping but absent in expected mapping");
        } else {
            var expectedFieldType = expectedFieldMapping.get("type");
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
        if (fieldSpecificMatcher == null) {
            return Optional.empty();
        }

        MatchResult matched = fieldSpecificMatcher.match(actualValues, expectedValues, expectedFieldMapping, actualFieldMapping);
        return Optional.of(matched);
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

    // We could look up the format from mapping eventually.
    private static long parseTimestampToEpochMillis(Object timestamp) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).parseMillis((String) timestamp);
    }
}
