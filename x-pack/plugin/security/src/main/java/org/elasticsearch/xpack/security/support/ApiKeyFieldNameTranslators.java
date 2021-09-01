/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import java.util.List;
import java.util.function.Function;

/**
 * A class to translate query level field names to index level field names.
 */
public class ApiKeyFieldNameTranslators {
    static final List<FieldNameTranslator> FIELD_NAME_TRANSLATORS;

    static {
        FIELD_NAME_TRANSLATORS = org.elasticsearch.core.List.of(
            new ExactFieldNameTranslator(s -> "creator.principal", "username"),
            new ExactFieldNameTranslator(s -> "creator.realm", "realm_name"),
            new ExactFieldNameTranslator(Function.identity(), "name"),
            new ExactFieldNameTranslator(s -> "creation_time", "creation"),
            new ExactFieldNameTranslator(s -> "expiration_time", "expiration"),
            new ExactFieldNameTranslator(s -> "api_key_invalidated", "invalidated"),
            new PrefixFieldNameTranslator(s -> "metadata_flattened" + s.substring(8), "metadata."));
    }

    /**
     * Translate the query level field name to index level field names.
     * It throws an exception if the field name is not explicitly allowed.
     */
    public static String translate(String fieldName) {
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(fieldName)) {
                return translator.translate(fieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + fieldName + "] is not allowed for API Key query");
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
        public boolean supports(String fieldName) {
            return name.equals(fieldName);
        }
    }

    static class PrefixFieldNameTranslator extends FieldNameTranslator {
        private final String prefix;

        PrefixFieldNameTranslator(Function<String, String> translationFunc, String prefix) {
            super(translationFunc);
            this.prefix = prefix;
        }

        @Override
        boolean supports(String fieldName) {
            return fieldName.startsWith(prefix);
        }
    }
}
