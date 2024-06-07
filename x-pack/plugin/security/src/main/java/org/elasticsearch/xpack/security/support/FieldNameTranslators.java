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

public final class FieldNameTranslators {

    final List<FieldNameTranslator> fieldNameTranslators;

    public FieldNameTranslators(List<FieldNameTranslator> fieldNameTranslators) {
        this.fieldNameTranslators = fieldNameTranslators;
    }

    /**
     * Translate the query level field name to index level field names.
     * It throws an exception if the field name is not explicitly allowed.
     */
    public String translate(String fieldName) {
        // protected for testing
        if (Regex.isSimpleMatchPattern(fieldName)) {
            throw new IllegalArgumentException("Field name pattern [" + fieldName + "] is not allowed for querying or aggregation");
        }
        for (FieldNameTranslator translator : fieldNameTranslators) {
            if (translator.supports(fieldName)) {
                return translator.translate(fieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + fieldName + "] is not allowed for querying or aggregation");
    }

    /**
     * Translates a query level field name pattern to the matching index level field names.
     * The result can be the empty set, if the pattern doesn't match any of the allowed index level field names.
     */
    public Set<String> translatePattern(String fieldNameOrPattern) {
        Set<String> indexFieldNames = new HashSet<>();
        for (FieldNameTranslator translator : fieldNameTranslators) {
            if (translator.supports(fieldNameOrPattern)) {
                indexFieldNames.add(translator.translate(fieldNameOrPattern));
            }
        }
        // It's OK to "translate" to the empty set the concrete disallowed or unknown field names.
        // For eg, the SimpleQueryString query type is lenient in the sense that it ignores unknown fields and field name patterns,
        // so this preprocessing can ignore them too.
        return indexFieldNames;
    }

    static abstract class FieldNameTranslator {

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
        boolean supports(String fieldNameOrPattern) {
            if (Regex.isSimpleMatchPattern(fieldNameOrPattern)) {
                // It is not possible to translate a pattern into concrete field names,
                // because we do not store the list of concrete field names sharing this same prefix.
                // That means that e.g. `metadata.*` and `metadata.x*` are expanded to the empty list,
                // rather than be replaced with `metadata_flattened.*` and `metadata_flattened.x*`
                // (but, in any case, `metadata_flattened.*` and `metadata.x*` are eventually ignored,
                // because of the way that the flattened field type works in ES)
                return false;
            }
            return fieldNameOrPattern.startsWith(prefix);
        }
    }
}
