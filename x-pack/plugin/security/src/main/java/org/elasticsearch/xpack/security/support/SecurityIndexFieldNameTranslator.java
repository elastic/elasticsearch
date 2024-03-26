/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class SecurityIndexFieldNameTranslator {

    private final List<SecurityIndexFieldNameTranslator.FieldName> fieldNameTranslators;

    public SecurityIndexFieldNameTranslator(List<SecurityIndexFieldNameTranslator.FieldName> fieldNameTranslators) {
        this.fieldNameTranslators = fieldNameTranslators;
    }

    public String translate(String queryFieldName) {
        for (FieldName fieldName : this.fieldNameTranslators) {
            if (fieldName.supportsQueryName(queryFieldName)) {
                return fieldName.indexFieldName(queryFieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + queryFieldName + "] is not allowed");
    }

    public boolean supportedIndexFieldName(String indexFieldName) {
        for (FieldName fieldName : this.fieldNameTranslators) {
            if (fieldName.supportsIndexName(indexFieldName)) {
                return true;
            }
        }
        return false;
    }

    public static FieldName exact(String name) {
        return exact(name, Function.identity());
    }

    public static FieldName exact(String name, Function<String, String> translation) {
        return new SecurityIndexFieldNameTranslator.ExactFieldName(name, translation);
    }

    public abstract static class FieldName {
        private final Function<String, String> toIndexFieldName;
        protected final Predicate<String> validIndexNamePredicate;

        FieldName(Function<String, String> toIndexFieldName, Predicate<String> validIndexNamePredicate) {
            this.toIndexFieldName = toIndexFieldName;
            this.validIndexNamePredicate = validIndexNamePredicate;
        }

        public abstract boolean supportsQueryName(String queryFieldName);

        public abstract boolean supportsIndexName(String indexFieldName);

        public String indexFieldName(String queryFieldName) {
            return toIndexFieldName.apply(queryFieldName);
        }
    }

    private static class ExactFieldName extends FieldName {
        private final String name;

        private ExactFieldName(String name, Function<String, String> toIndexFieldName) {
            super(toIndexFieldName, fieldName -> toIndexFieldName.apply(name).equals(fieldName));
            this.name = name;
        }

        @Override
        public boolean supportsQueryName(String queryFieldName) {
            return queryFieldName.equals(name);
        }

        @Override
        public boolean supportsIndexName(String indexFieldName) {
            return validIndexNamePredicate.test(indexFieldName);
        }
    }
}
