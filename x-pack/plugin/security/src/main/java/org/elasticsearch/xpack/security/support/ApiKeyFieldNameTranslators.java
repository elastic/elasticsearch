/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.regex.Regex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.action.apikey.TransportQueryApiKeyAction.API_KEY_TYPE_RUNTIME_MAPPING_FIELD;

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
            new ExactFieldNameTranslator(s -> API_KEY_TYPE_RUNTIME_MAPPING_FIELD, "type"),
            new ExactFieldNameTranslator(s -> "creation_time", "creation"),
            new ExactFieldNameTranslator(s -> "expiration_time", "expiration"),
            new ExactFieldNameTranslator(s -> "api_key_invalidated", "invalidated"),
            new ExactFieldNameTranslator(s -> "invalidation_time", "invalidation"),
            // allows querying on all metadata values as keywords because "metadata_flattened" is a flattened field type
            new ExactFieldNameTranslator(s -> "metadata_flattened", "metadata"),
            new PrefixFieldNameTranslator(s -> "metadata_flattened." + s.substring("metadata.".length()), "metadata.")
        );
    }

    /**
     * Translate the query level field name to index level field names.
     * It throws an exception if the field name is not explicitly allowed.
     */
    public static String translate(String fieldName) {
        if (Regex.isSimpleMatchPattern(fieldName)) {
            throw new IllegalArgumentException("Field name pattern [" + fieldName + "] is not allowed for API Key query");
        }
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(fieldName)) {
                return translator.translate(fieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + fieldName + "] is not allowed for API Key query");
    }

    /**
     * Translates a query level field name pattern to the matching index level field names.
     * The result can be the empty set, if the pattern doesn't match any of the allowed index level field names.
     * If the pattern is actually a concrete field name rather than a pattern,
     * it is also translated, but only if the query level field name is allowed, otherwise an exception is thrown.
     */
    public static Set<String> translatePattern(String fieldNameOrPattern) {
        Set<String> indexFieldNames = new HashSet<>();
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(fieldNameOrPattern)) {
                indexFieldNames.add(translator.translate(fieldNameOrPattern));
            }
        }
        // It's OK to "translate" to the empty set the concrete disallowed or unknown field names, because
        // the SimpleQueryString query type is lenient in the sense that it ignores unknown fields and field name patterns,
        // so this preprocessing can ignore them too.
        return indexFieldNames;
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
            this.prefix = prefix;
        }

        @Override
        boolean supports(String fieldNamePrefix) {
            // a pattern can generally match a prefix in multiple ways
            // moreover, it's not possible to iterate the concrete fields matching the prefix
            if (Regex.isSimpleMatchPattern(fieldNamePrefix)) {
                // this means that e.g. `metadata.*` and `metadata.x*` are expanded to the empty list,
                // rather than be replaced with `metadata_flattened.*` and `metadata_flattened.x*`
                // (but, in any case, `metadata_flattened.*` and `metadata.x*` are going to be ignored)
                return false;
            }
            return fieldNamePrefix.startsWith(prefix);
        }
    }
}
