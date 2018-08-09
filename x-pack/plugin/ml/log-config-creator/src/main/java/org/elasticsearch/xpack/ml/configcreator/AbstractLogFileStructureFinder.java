/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractLogFileStructureFinder {

    protected static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";
    protected static final String MAPPING_TYPE_SETTING = "type";
    protected static final String MAPPING_FORMAT_SETTING = "format";
    protected static final String MAPPING_PROPERTIES_SETTING = "properties";

    // NUMBER Grok pattern doesn't support scientific notation, so we extend it
    private static final Grok NUMBER_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{NUMBER}(?:[eE][+-]?[0-3]?[0-9]{1,2})?$");
    private static final Grok IP_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{IP}$");
    private static final int KEYWORD_MAX_LEN = 256;
    private static final int KEYWORD_MAX_SPACES = 5;

    protected final Terminal terminal;

    protected AbstractLogFileStructureFinder(Terminal terminal) {
        this.terminal = Objects.requireNonNull(terminal);
    }

    protected static Map<String, String> guessScalarMapping(Terminal terminal, String fieldName, Collection<String> fieldValues) {

        if (fieldValues.stream().allMatch(value -> "true".equals(value) || "false".equals(value))) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "boolean");
        }

        TimestampMatch singleMatch = null;
        for (String fieldValue : fieldValues) {
            if (singleMatch == null) {
                singleMatch = TimestampFormatFinder.findFirstFullMatch(fieldValue);
                if (singleMatch == null) {
                    break;
                }
            } else if (singleMatch.equals(TimestampFormatFinder.findFirstFullMatch(fieldValue, singleMatch.candidateIndex)) == false) {
                singleMatch = null;
                break;
            }
        }
        if (singleMatch != null) {
            return singleMatch.getEsDateMappingTypeWithFormat();
        }

        if (fieldValues.stream().allMatch(NUMBER_GROK::match)) {
            try {
                fieldValues.forEach(Long::parseLong);
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "long");
            } catch (NumberFormatException e) {
                terminal.println(Verbosity.VERBOSE,
                    "Rejecting type 'long' for field [" + fieldName + "] due to parse failure: [" + e.getMessage() + "]");
            }
            try {
                fieldValues.forEach(Double::parseDouble);
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "double");
            } catch (NumberFormatException e) {
                terminal.println(Verbosity.VERBOSE,
                    "Rejecting type 'double' for field [" + fieldName + "] due to parse failure: [" + e.getMessage() + "]");
            }
        }

        else if (fieldValues.stream().allMatch(IP_GROK::match)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "ip");
        }

        if (fieldValues.stream().anyMatch(AbstractStructuredLogFileStructureFinder::isMoreLikelyTextThanKeyword)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "text");
        }

        return Collections.singletonMap(MAPPING_TYPE_SETTING, "keyword");
    }

    static boolean isMoreLikelyTextThanKeyword(String str) {
        int length = str.length();
        return length > KEYWORD_MAX_LEN || length - str.replaceAll("\\s", "").length() > KEYWORD_MAX_SPACES;
    }
}
