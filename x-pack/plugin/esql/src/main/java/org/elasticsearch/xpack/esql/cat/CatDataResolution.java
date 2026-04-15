/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.cat;

import org.elasticsearch.common.Table;

import java.util.Map;

/**
 * Holds pre-fetched CAT data (one {@link Table} per requested endpoint) that is
 * carried from the pre-analysis phase into the {@link org.elasticsearch.xpack.esql.analysis.Analyzer}.
 */
public record CatDataResolution(Map<String, Table> tables) {

    public static final CatDataResolution EMPTY = new CatDataResolution(Map.of());

    /** Returns the fetched table for {@code endpoint}, or {@code null} if not present. */
    public Table get(String endpoint) {
        return tables.get(endpoint);
    }
}
