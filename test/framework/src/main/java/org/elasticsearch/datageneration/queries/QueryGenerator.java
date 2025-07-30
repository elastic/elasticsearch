/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class QueryGenerator {

    private final Map<String, Map<String, Object>> mappingLookup;
    private final Map<String, Object> mappingRaw;

    public QueryGenerator(Map<String, Map<String, Object>> mappingLookup, Map<String, Object> mappingRaw) {
        this.mappingLookup = mappingLookup;
        this.mappingRaw = mappingRaw;
    }

    public List<QueryBuilder> generateQueries(String path, Map<String, Object> mapping, Object value) {
        // This query generator cannot handle fields with periods in the name.
        if (path.equals("host.name")) {
            return List.of();
        }
        if (mapping == null || isEnabled(path) == false) {
            return List.of();
        }
        boolean isIndexed = (Boolean) mapping.getOrDefault("index", true);
        if (isIndexed == false) {
            return List.of();
        }
        var type = (String) mapping.get("type");
        var leafQueryGenerator = LeafQueryGenerator.buildForType(type);
        var leafQueries = leafQueryGenerator.generate(mapping, path, value);
        return leafQueries.stream().map(q -> wrapInNestedQuery(path, q)).toList();
    }

    private QueryBuilder wrapInNestedQuery(String path, QueryBuilder leafQuery) {
        String[] parts = path.split("\\.");
        List<String> nestedPaths = getNestedPathPrefixes(parts);
        QueryBuilder query = leafQuery;
        for (String nestedPath : nestedPaths.reversed()) {
            query = QueryBuilders.nestedQuery(nestedPath, query, ScoreMode.Max);
        }
        return query;
    }

    @SuppressWarnings("unchecked")
    private List<String> getNestedPathPrefixes(String[] path) {
        Map<String, Object> mapping = mappingRaw;
        mapping = (Map<String, Object>) mapping.get("_doc");
        mapping = (Map<String, Object>) mapping.get("properties");

        var result = new ArrayList<String>();
        for (int i = 0; i < path.length - 1; i++) {
            var field = path[i];
            mapping = (Map<String, Object>) mapping.get(field);
            boolean nested = "nested".equals(mapping.get("type"));
            if (nested) {
                result.add(String.join(".", Arrays.copyOfRange(path, 0, i + 1)));
            }
            mapping = (Map<String, Object>) mapping.get("properties");
        }

        mapping = (Map<String, Object>) mapping.get(path[path.length - 1]);
        assert mapping.containsKey("properties") == false;
        return result;
    }

    /**
     * Traverse down mapping tree and check that all objects are enabled in path
     */
    private boolean isEnabled(String path) {
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length - 1; i++) {
            var pathToHere = String.join(".", Arrays.copyOfRange(parts, 0, i + 1));
            Map<String, Object> mapping = mappingLookup.get(pathToHere);

            boolean enabled = true;
            if (mapping.containsKey("enabled") && mapping.get("enabled") instanceof Boolean) {
                enabled = (Boolean) mapping.get("enabled");
            }
            if (mapping.containsKey("enabled") && mapping.get("enabled") instanceof String) {
                enabled = Booleans.parseBoolean((String) mapping.get("enabled"));
            }

            if (enabled == false) {
                return false;
            }
        }
        return true;
    }
}
