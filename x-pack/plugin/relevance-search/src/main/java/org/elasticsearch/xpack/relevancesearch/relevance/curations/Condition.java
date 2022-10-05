/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.curations;

import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;

public abstract class Condition {

    private final String value;

    protected Condition(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must be specified");
        }
        this.value = value;
    }

    public static Condition buildCondition(String context, String value) {
        if (context == null) {
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

    public static class QueryCondition extends Condition {

        public QueryCondition(String value) {
            super(normalizeValue(value));
        }

        @Override
        public boolean match(RelevanceMatchQueryBuilder queryBuilder) {
            return getValue().equals(normalizeValue(queryBuilder.getQuery()));
        }

        private static String normalizeValue(String value) {
            if (value == null) {
                return value;
            }

            return value.toLowerCase().trim();
        }
    }
}
