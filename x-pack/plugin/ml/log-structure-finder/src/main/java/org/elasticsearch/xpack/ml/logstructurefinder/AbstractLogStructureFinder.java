/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.ml.logstructurefinder.TimestampFormatFinder.TimestampMatch;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbstractLogStructureFinder {

    public static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";
    public static final String MAPPING_TYPE_SETTING = "type";
    public static final String MAPPING_FORMAT_SETTING = "format";
    public static final String MAPPING_PROPERTIES_SETTING = "properties";

    // NUMBER Grok pattern doesn't support scientific notation, so we extend it
    private static final Grok NUMBER_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{NUMBER}(?:[eE][+-]?[0-3]?[0-9]{1,2})?$");
    private static final Grok IP_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{IP}$");
    private static final int KEYWORD_MAX_LEN = 256;
    private static final int KEYWORD_MAX_SPACES = 5;

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
    protected static Map<String, String> guessScalarMapping(List<String> explanation, String fieldName, Collection<String> fieldValues) {

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

        if (fieldValues.stream().anyMatch(AbstractStructuredLogStructureFinder::isMoreLikelyTextThanKeyword)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "text");
        }

        return Collections.singletonMap(MAPPING_TYPE_SETTING, "keyword");
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
