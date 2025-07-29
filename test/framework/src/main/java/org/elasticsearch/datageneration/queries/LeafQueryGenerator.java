/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.queries;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface LeafQueryGenerator {

    List<QueryBuilder> generate(Map<String, Object> fieldMapping, String path, Object value);

    /**
     * Build a query for a specific type. If the field is nested, this query will need to be wrapped in nested queries.
     * @param type the type to build a query for
     * @return a generator that can build queries for this type
     */
    static LeafQueryGenerator buildForType(String type) {
        LeafQueryGenerator noQueries = (Map<String, Object> fieldMapping, String path, Object value) -> List.of();

        FieldType fieldType = FieldType.tryParse(type);
        if (fieldType == null) {
            return noQueries;
        }

        return switch (fieldType) {
            case KEYWORD -> new KeywordQueryGenerator();
            case TEXT -> new TextQueryGenerator();
            case WILDCARD -> new WildcardQueryGenerator();
            default -> noQueries;
        };
    }

    class KeywordQueryGenerator implements LeafQueryGenerator {
        public List<QueryBuilder> generate(Map<String, Object> fieldMapping, String path, Object value) {
            var ignoreAbove = (Integer) fieldMapping.getOrDefault("ignore_above", Integer.MAX_VALUE);
            var s = (String) value;
            if (s.isEmpty() || ignoreAbove < s.length()) {
                return List.of();
            }
            return List.of(QueryBuilders.termQuery(path, value));
        }
    }

    class WildcardQueryGenerator implements LeafQueryGenerator {
        public List<QueryBuilder> generate(Map<String, Object> fieldMapping, String path, Object value) {
            var ignoreAbove = (Integer) fieldMapping.getOrDefault("ignore_above", Integer.MAX_VALUE);
            var s = (String) value;
            if (s.isEmpty() || ignoreAbove < s.length()) {
                return List.of();
            }
            return List.of(QueryBuilders.termQuery(path, value), QueryBuilders.wildcardQuery(path, value + "*"));
        }
    }

    class TextQueryGenerator implements LeafQueryGenerator {
        public List<QueryBuilder> generate(Map<String, Object> fieldMapping, String path, Object value) {
            if (((String) value).isEmpty()) {
                return List.of();
            }
            var results = new ArrayList<QueryBuilder>();
            results.add(QueryBuilders.matchQuery(path, value));
            var phraseQuery = buildPhraseQuery(path, value);
            if (phraseQuery != null) {
                results.add(phraseQuery);
            }
            return results;
        }

        private static QueryBuilder buildPhraseQuery(String path, Object value) {
            String needle = (String) value;
            var tokens = Arrays.asList(needle.split("[^a-zA-Z0-9]"));
            if (tokens.isEmpty()) {
                return null;
            }

            int low = ESTestCase.randomIntBetween(0, tokens.size() - 1);
            int hi = ESTestCase.randomIntBetween(low + 1, tokens.size());
            var phrase = String.join(" ", tokens.subList(low, hi));
            return QueryBuilders.matchPhraseQuery(path, phrase);
        }
    }
}
