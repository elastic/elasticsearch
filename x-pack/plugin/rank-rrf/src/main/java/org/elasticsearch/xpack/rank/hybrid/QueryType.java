/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import java.util.Locale;

public enum QueryType {
    MATCH,
    MATCH_PHRASE;

    public String getQueryName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static QueryType fromString(String queryName) {
        return valueOf(queryName.toUpperCase(Locale.ROOT));
    }
}
