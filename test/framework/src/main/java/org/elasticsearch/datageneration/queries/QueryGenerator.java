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
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class QueryGenerator {

    private final Mapping mapping;
    private final MappingContextHelper mappingContextHelper;

    public QueryGenerator(Mapping mapping) {
        this.mapping = mapping;
        this.mappingContextHelper = new MappingContextHelper(mapping);
    }

    public List<QueryBuilder> generateQueries(String type, String path, Object value) {
        // This query generator cannot handle fields with periods in the name.
        if (path.equals("host.name")) {
            return List.of();
        }
        // Can handle dynamically mapped fields, but not runtime fields
        if (mappingContextHelper.isRuntimeField(path)) {
            return List.of();
        }
        var leafQueryGenerator = LeafQueryGenerator.buildForType(type, mappingContextHelper);
        var fieldMapping = mapping.lookup().get(path);
        var leafQueries = leafQueryGenerator.generate(fieldMapping, path, value);
        return leafQueries.stream().map(q -> wrapInNestedQuery(path, q)).toList();
    }

    public QueryBuilder wrapInNestedQuery(String path, QueryBuilder leafQuery) {
        String[] parts = path.split("\\.");
        List<String> nestedPaths = mappingContextHelper.getNestedPathPrefixes(parts);
        QueryBuilder query = leafQuery;
        for (String nestedPath : nestedPaths.reversed()) {
            query = QueryBuilders.nestedQuery(nestedPath, query, ScoreMode.Max);
        }
        return query;
    }
}
