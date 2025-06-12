/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.apache.commons.text.StringSubstitutor;
import org.elasticsearch.common.Strings;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Substitutes placeholder values in a string that match the keys in a provided map with the map's corresponding values.
 */
public class ValidatingSubstitutor {
    /**
     * This regex pattern matches on the string {@code ${<any characters>}} excluding newlines.
     */
    private static final Pattern VARIABLE_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{.*?\\}");

    private final StringSubstitutor substitutor;

    /**
     * @param params a map containing the placeholders as the keys, the values will be used to replace the placeholders
     * @param prefix a string indicating the start of a placeholder
     * @param suffix a string indicating the end of a placeholder
     */
    public ValidatingSubstitutor(Map<String, String> params, String prefix, String suffix) {
        substitutor = new StringSubstitutor(params, prefix, suffix);
    }

    /**
     * Substitutes placeholder values in a string that match the keys in a provided map with the map's corresponding values.
     * After replacement, if the source still contains a placeholder an {@link IllegalStateException} is thrown.
     * @param source the string that will be searched for placeholders to be replaced
     * @param field a description of the source string
     * @return a string with the placeholders replaced by string values
     */
    public String replace(String source, String field) {
        var replacedString = substitutor.replace(source);
        ensureNoMorePlaceholdersExist(replacedString, field);
        return replacedString;
    }

    private static void ensureNoMorePlaceholdersExist(String substitutedString, String field) {
        Matcher matcher = VARIABLE_PLACEHOLDER_PATTERN.matcher(substitutedString);
        if (matcher.find()) {
            throw new IllegalStateException(
                Strings.format("Found placeholder [%s] in field [%s] after replacement call", matcher.group(), field)
            );
        }
    }
}
