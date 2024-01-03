/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A class to translate query level field names to index level field names.
 */
public class ApiKeyFieldNameTranslators {
    static final List<FieldNameTranslator> FIELD_NAME_TRANSLATORS;

    static {
        FIELD_NAME_TRANSLATORS = List.of(
            new ExactFieldNameTranslator(s -> "creator.principal", "username"),
            new ExactFieldNameTranslator(s -> "creator.realm", "realm_name"),
            new ExactFieldNameTranslator(s -> "name", "name"),
            new ExactFieldNameTranslator(s -> "creation_time", "creation"),
            new ExactFieldNameTranslator(s -> "expiration_time", "expiration"),
            new ExactFieldNameTranslator(s -> "api_key_invalidated", "invalidated"),
            new ExactFieldNameTranslator(s -> "invalidation_time", "invalidation"),
            new ExactFieldNameTranslator(s -> "metadata_flattened", "metadata"), // allows querying on all metadata values as keywords
            new PrefixFieldNameTranslator(s -> "metadata_flattened." + s.substring("metadata.".length()), "metadata.")
        );
    }

    /**
     * Translates the query level field name to index level field names.
     * It throws an exception if the field name is not allowed to be queried.
     */
    public static String translate(String fieldName) {
        if (Regex.isSimpleMatchPattern(fieldName)) {
            throw new IllegalArgumentException("Field name pattern not supported");
        }
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(fieldName)) {
                return translator.translate(fieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + fieldName + "] is not allowed for API Key query");
    }

    /**
     * Translates the query level field name pattern to index level field names that match the pattern.
     */
    public static Set<String> translatePattern(String pattern) {
        Set<String> translatedPatternMatches = new HashSet<>();
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(pattern)) {
                translatedPatternMatches.add(translator.translate(pattern));
            }
        }
        return translatedPatternMatches;
    }

    abstract static class FieldNameTranslator {

        private final Function<String, String> translationFunc;

        protected FieldNameTranslator(Function<String, String> translationFunc) {
            this.translationFunc = translationFunc;
        }

        String translate(String fieldName) {
            return translationFunc.apply(fieldName);
        }

        abstract boolean supports(String fieldName);
    }

    static class ExactFieldNameTranslator extends FieldNameTranslator {
        private final String name;

        ExactFieldNameTranslator(Function<String, String> translationFunc, String name) {
            super(translationFunc);
            assert Regex.isSimpleMatchPattern(name) == false : "unsupported pattern as field name";
            this.name = name;
        }

        @Override
        public boolean supports(String fieldNameOrPattern) {
            if (Regex.isSimpleMatchPattern(fieldNameOrPattern)) {
                return Regex.simpleMatch(fieldNameOrPattern, name);
            } else {
                return name.equals(fieldNameOrPattern);
            }
        }
    }

    static class PrefixFieldNameTranslator extends FieldNameTranslator {
        private final String prefix;

        PrefixFieldNameTranslator(Function<String, String> translationFunc, String prefix) {
            super(translationFunc);
            assert Regex.isSimpleMatchPattern(prefix) == false : "unsupported pattern as field name prefix";
            this.prefix = prefix;
        }

        @Override
        boolean supports(String fieldName) {
            // it is not easily possible to translate field names that both start with a prefix AND that match a regex pattern
            if (Regex.isSimpleMatchPattern(fieldName)) {
                return false;
            }
            return fieldName.startsWith(prefix);
        }
    }
}
