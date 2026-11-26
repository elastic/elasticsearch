/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * Query read shapes, first-class per elastic/esql-planning#1650. Every corpus workload must cover
 * all four (validator-enforced), including trimmed {@code querySubset} legs.
 */
public enum ReadShape {
    SCAN,
    AGGREGATE,
    TOPN,
    LIMIT;

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ReadShape fromId(String id) {
        return valueOf(id.toUpperCase(Locale.ROOT));
    }
}
