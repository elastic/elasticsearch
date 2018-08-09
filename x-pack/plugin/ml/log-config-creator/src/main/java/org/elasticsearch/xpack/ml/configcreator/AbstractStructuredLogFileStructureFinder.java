/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractStructuredLogFileStructureFinder extends AbstractLogFileStructureFinder {

    protected Tuple<String, TimestampMatch> guessTimestampField(List<String> explanation, List<Map<String, ?>> sampleRecords) {
        if (sampleRecords == null || sampleRecords.isEmpty()) {
            return null;
        }

        List<Tuple<String, TimestampMatch>> firstSampleMatches = new ArrayList<>();

        // Get candidate timestamps from the first sample record
        for (Map.Entry<String, ?> entry : sampleRecords.get(0).entrySet()) {
            Object value = entry.getValue();
            if (value != null) {
                TimestampMatch match = TimestampFormatFinder.findFirstFullMatch(value.toString());
                if (match != null) {
                    Tuple<String, TimestampMatch> firstSampleMatch = new Tuple<>(entry.getKey(), match);
                    // If there's only one sample then the first match is the time field
                    if (sampleRecords.size() == 1) {
                        return firstSampleMatch;
                    }
                    firstSampleMatches.add(firstSampleMatch);
                    explanation.add("First sample timestamp match [" + firstSampleMatch + "]");
                }
            }
        }

        // Accept the first match from the first sample that is compatible with all the other samples
        for (Tuple<String, TimestampMatch> firstSampleMatch : firstSampleMatches) {

            boolean allGood = true;
            for (Map<String, ?> sampleRecord : sampleRecords.subList(1, sampleRecords.size())) {
                Object fieldValue = sampleRecord.get(firstSampleMatch.v1());
                if (fieldValue == null) {
                    explanation.add("First sample match [" + firstSampleMatch.v1() + "] ruled out because record [" + sampleRecord +
                        "] doesn't have field");
                    allGood = false;
                    break;
                }

                TimestampMatch match = TimestampFormatFinder.findFirstFullMatch(fieldValue.toString());
                if (match == null || match.candidateIndex != firstSampleMatch.v2().candidateIndex) {
                    explanation.add("First sample match [" + firstSampleMatch.v1() + "] ruled out because record [" + sampleRecord +
                        "] matches differently: [" + match + "]");
                    allGood = false;
                    break;
                }
            }

            if (allGood) {
                explanation.add("Guessing timestamp field is [" + firstSampleMatch.v1() + "] with format [" + firstSampleMatch.v2() + "]");
                return firstSampleMatch;
            }
        }

        return null;
    }

    /**
     * Given the sampled record, guess appropriate Elasticsearch mappings.
     * @param sampleRecords The sampled records.
     * @return A map of field name to mapping settings.
     */
    protected SortedMap<String, Object> guessMappings(List<String> explanation, List<Map<String, ?>> sampleRecords) throws UserException {

        SortedMap<String, Object> mappings = new TreeMap<>();

        if (sampleRecords != null) {

            try {
                for (Map<String, ?> sampleRecord : sampleRecords) {
                    for (String fieldName : sampleRecord.keySet()) {
                        mappings.computeIfAbsent(fieldName, key -> guessMapping(explanation, fieldName,
                            sampleRecords.stream().flatMap(record -> {
                                Object fieldValue = record.get(fieldName);
                                return (fieldValue == null) ? Stream.empty() : Stream.of(fieldValue);
                            }
                        ).collect(Collectors.toList())));
                    }
                }
            } catch (RuntimeException e) {
                throw new UserException(ExitCodes.DATA_ERROR, e.getMessage(), e);
            }
        }

        return mappings;
    }

    Map<String, String> guessMapping(List<String> explanation, String fieldName, List<Object> fieldValues) {

        if (fieldValues == null || fieldValues.isEmpty()) {
            // We can get here if all the records that contained a given field had a null value for it.
            // In this case it's best not to make any statement about what the mapping type should be.
            return null;
        }

        if (fieldValues.stream().anyMatch(value -> value instanceof Map)) {
            if (fieldValues.stream().allMatch(value -> value instanceof Map)) {
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "object");
            }
            throw new RuntimeException("Field [" + fieldName +
                "] has both object and non-object values - this won't work with Elasticsearch");
        }

        if (fieldValues.stream().anyMatch(value -> value instanceof List || value instanceof Object[])) {
            // Elasticsearch fields can be either arrays or single values, but array values must all have the same type
            return guessMapping(explanation, fieldName,
                fieldValues.stream().flatMap(AbstractStructuredLogFileStructureFinder::flatten).collect(Collectors.toList()));
        }

        return guessScalarMapping(explanation, fieldName, fieldValues.stream().map(Object::toString).collect(Collectors.toList()));
    }

    private static Stream<Object> flatten(Object value) {
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> objectList = (List<Object>) value;
            return objectList.stream();
        } else if (value instanceof Object[]) {
            return Arrays.stream((Object[]) value);
        } else {
            return Stream.of(value);
        }
    }
}
