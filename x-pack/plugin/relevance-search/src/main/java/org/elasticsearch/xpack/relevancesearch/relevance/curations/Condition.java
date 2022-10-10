/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.curations;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;

import java.util.Locale;

/**
 * Conditions used to match a curation. This is a base class that can be implemented for different types of conditions.
 */
public abstract class Condition {

    private final String value;

    protected Condition(String value) {
        if (Strings.isEmpty(value)) {
            throw new IllegalArgumentException("Value must be specified");
        }
        this.value = value;
    }

    public static Condition buildCondition(String context, String value) {
        if (Strings.isEmpty(context)) {
            throw new IllegalArgumentException("Context must be specified");
        }
        return switch (context) {
            case "query" -> new QueryCondition(value);
            default -> throw new IllegalArgumentException("Unknown context: " + context);
        };
    }

    public String getValue() {
        return value;
    }

    public abstract boolean match(RelevanceMatchQueryBuilder queryBuilder);

    /**
     * A condition that relies on the query exactly matching the value
     */
    public static class QueryCondition extends Condition {

        public QueryCondition(String value) {
            super(normalizeValue(value));
        }

        @Override
        public boolean match(RelevanceMatchQueryBuilder queryBuilder) {
            return getValue().equals(normalizeValue(queryBuilder.getQuery()));
        }

        private static String normalizeValue(String value) {
            return value.toLowerCase(Locale.ROOT).trim();
        }
    }
}
