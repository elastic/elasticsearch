/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.ml.logstructurefinder.TimestampFormatFinder.TimestampMatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LogStructureUtils {

    public static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";
    public static final String MAPPING_TYPE_SETTING = "type";
    public static final String MAPPING_FORMAT_SETTING = "format";
    public static final String MAPPING_PROPERTIES_SETTING = "properties";

    private static final int NUM_TOP_HITS = 10;
    // NUMBER Grok pattern doesn't support scientific notation, so we extend it
    private static final Grok NUMBER_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{NUMBER}(?:[eE][+-]?[0-3]?[0-9]{1,2})?$");
    private static final Grok IP_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{IP}$");
    private static final int KEYWORD_MAX_LEN = 256;
    private static final int KEYWORD_MAX_SPACES = 5;

    private LogStructureUtils() {
    }

    /**
     * Given one or more sample records, find a timestamp field that is consistently present in them all.
     * To be returned the timestamp field:
     * - Must exist in every record
     * - Must have the same timestamp format in every record
     * If multiple fields meet these criteria then the one that occurred first in the first sample record
     * is chosen.
     * @param explanation List of reasons for choosing the overall log structure.  This list
     *                    may be non-empty when the method is called, and this method may
     *                    append to it.
     * @param sampleRecords List of records derived from the provided log sample.
     * @return A tuple of (field name, timestamp format) if one can be found, or <code>null</code> if
     *         there is no consistent timestamp.
     */
    static Tuple<String, TimestampMatch> guessTimestampField(List<String> explanation, List<Map<String, ?>> sampleRecords) {
        if (sampleRecords.isEmpty()) {
            return null;
        }

        // Accept the first match from the first sample that is compatible with all the other samples
        for (Tuple<String, TimestampMatch> candidate : findCandidates(explanation, sampleRecords)) {

            boolean allGood = true;
            for (Map<String, ?> sampleRecord : sampleRecords.subList(1, sampleRecords.size())) {
                Object fieldValue = sampleRecord.get(candidate.v1());
                if (fieldValue == null) {
                    explanation.add("First sample match [" + candidate.v1() + "] ruled out because record [" + sampleRecord +
                        "] doesn't have field");
                    allGood = false;
                    break;
                }

                TimestampMatch match = TimestampFormatFinder.findFirstFullMatch(fieldValue.toString());
                if (match == null || match.candidateIndex != candidate.v2().candidateIndex) {
                    explanation.add("First sample match [" + candidate.v1() + "] ruled out because record [" + sampleRecord +
                        "] matches differently: [" + match + "]");
                    allGood = false;
                    break;
                }
            }

            if (allGood) {
                explanation.add("Guessing timestamp field is [" + candidate.v1() + "] with format [" + candidate.v2() + "]");
                return candidate;
            }
        }

        return null;
    }

    private static List<Tuple<String, TimestampMatch>> findCandidates(List<String> explanation, List<Map<String, ?>> sampleRecords) {

        List<Tuple<String, TimestampMatch>> candidates = new ArrayList<>();

        // Get candidate timestamps from the first sample record
        for (Map.Entry<String, ?> entry : sampleRecords.get(0).entrySet()) {
            Object value = entry.getValue();
            if (value != null) {
                TimestampMatch match = TimestampFormatFinder.findFirstFullMatch(value.toString());
                if (match != null) {
                    Tuple<String, TimestampMatch> candidate = new Tuple<>(entry.getKey(), match);
                    candidates.add(candidate);
                    explanation.add("First sample timestamp match [" + candidate + "]");
                }
            }
        }

        return candidates;
    }

    /**
     * Given the sampled records, guess appropriate Elasticsearch mappings.
     * @param sampleRecords The sampled records.
     * @return A map of field name to mapping settings.
     */
    static Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>>
        guessMappingsAndCalculateFieldStats(List<String> explanation, List<Map<String, ?>> sampleRecords) {

        SortedMap<String, Object> mappings = new TreeMap<>();
        SortedMap<String, FieldStats> fieldStats = new TreeMap<>();

        Set<String> uniqueFieldNames = sampleRecords.stream().flatMap(record -> record.keySet().stream()).collect(Collectors.toSet());

        for (String fieldName : uniqueFieldNames) {

            List<Object> fieldValues = sampleRecords.stream().flatMap(record -> {
                    Object fieldValue = record.get(fieldName);
                    return (fieldValue == null) ? Stream.empty() : Stream.of(fieldValue);
                }
            ).collect(Collectors.toList());

            Tuple<Map<String, String>, FieldStats> mappingAndFieldStats =
                guessMappingAndCalculateFieldStats(explanation, fieldName, fieldValues);
            if (mappingAndFieldStats != null) {
                if (mappingAndFieldStats.v1() != null) {
                    mappings.put(fieldName, mappingAndFieldStats.v1());
                }
                if (mappingAndFieldStats.v2() != null) {
                    fieldStats.put(fieldName, mappingAndFieldStats.v2());
                }
            }
        }

        return new Tuple<>(mappings, fieldStats);
    }

    static Tuple<Map<String, String>, FieldStats> guessMappingAndCalculateFieldStats(List<String> explanation,
                                                                                     String fieldName, List<Object> fieldValues) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            // We can get here if all the records that contained a given field had a null value for it.
            // In this case it's best not to make any statement about what the mapping type should be.
            return null;
        }

        if (fieldValues.stream().anyMatch(value -> value instanceof Map)) {
            if (fieldValues.stream().allMatch(value -> value instanceof Map)) {
                return new Tuple<>(Collections.singletonMap(MAPPING_TYPE_SETTING, "object"), null);
            }
            throw new IllegalArgumentException("Field [" + fieldName +
                "] has both object and non-object values - this is not supported by Elasticsearch");
        }

        if (fieldValues.stream().anyMatch(value -> value instanceof List || value instanceof Object[])) {
            // Elasticsearch fields can be either arrays or single values, but array values must all have the same type
            return guessMappingAndCalculateFieldStats(explanation, fieldName,
                fieldValues.stream().flatMap(LogStructureUtils::flatten).collect(Collectors.toList()));
        }

        Collection<String> fieldValuesAsStrings = fieldValues.stream().map(Object::toString).collect(Collectors.toList());
        return new Tuple<>(guessScalarMapping(explanation, fieldName, fieldValuesAsStrings), calculateFieldStats(fieldValuesAsStrings));
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

    /**
     * Given some sample values for a field, guess the most appropriate index mapping for the
     * field.
     * @param explanation List of reasons for choosing the overall log structure.  This list
     *                    may be non-empty when the method is called, and this method may
     *                    append to it.
     * @param fieldName Name of the field for which mappings are to be guessed.
     * @param fieldValues Values of the field for which mappings are to be guessed.  The guessed
     *                    mapping will be compatible with all the provided values.  Must not be
     *                    empty.
     * @return The sub-section of the index mappings most appropriate for the field,
     *         for example <code>{ "type" : "keyword" }</code>.
     */
    static Map<String, String> guessScalarMapping(List<String> explanation, String fieldName, Collection<String> fieldValues) {

        assert fieldValues.isEmpty() == false;

        if (fieldValues.stream().allMatch(value -> "true".equals(value) || "false".equals(value))) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "boolean");
        }

        // This checks if a date mapping would be appropriate, and, if so, finds the correct format
        Iterator<String> iter = fieldValues.iterator();
        TimestampMatch timestampMatch = TimestampFormatFinder.findFirstFullMatch(iter.next());
        while (timestampMatch != null && iter.hasNext()) {
            // To be mapped as type date all the values must match the same date format - it is
            // not acceptable for all values to be dates, but with different formats
            if (timestampMatch.equals(TimestampFormatFinder.findFirstFullMatch(iter.next(), timestampMatch.candidateIndex)) == false) {
                timestampMatch = null;
            }
        }
        if (timestampMatch != null) {
            return timestampMatch.getEsDateMappingTypeWithFormat();
        }

        if (fieldValues.stream().allMatch(NUMBER_GROK::match)) {
            try {
                fieldValues.forEach(Long::parseLong);
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "long");
            } catch (NumberFormatException e) {
                explanation.add("Rejecting type 'long' for field [" + fieldName + "] due to parse failure: [" + e.getMessage() + "]");
            }
            try {
                fieldValues.forEach(Double::parseDouble);
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "double");
            } catch (NumberFormatException e) {
                explanation.add("Rejecting type 'double' for field [" + fieldName + "] due to parse failure: [" + e.getMessage() + "]");
            }
        }
        else if (fieldValues.stream().allMatch(IP_GROK::match)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "ip");
        }

        if (fieldValues.stream().anyMatch(LogStructureUtils::isMoreLikelyTextThanKeyword)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "text");
        }

        return Collections.singletonMap(MAPPING_TYPE_SETTING, "keyword");
    }

    /**
     * Calculate stats for a set of field values.
     * @param fieldValues Values of the field for which field stats are to be calculated.
     * @return The stats calculated from the field values.
     */
    static FieldStats calculateFieldStats(Collection<String> fieldValues) {

        FieldStatsCalculator calculator = new FieldStatsCalculator();
        calculator.accept(fieldValues);
        return calculator.calculate(NUM_TOP_HITS);
    }

    /**
     * The thinking is that the longer the field value and the more spaces it contains,
     * the more likely it is that it should be indexed as text rather than keyword.
     */
    static boolean isMoreLikelyTextThanKeyword(String str) {
        int length = str.length();
        return length > KEYWORD_MAX_LEN || length - str.replaceAll("\\s", "").length() > KEYWORD_MAX_SPACES;
    }
}
