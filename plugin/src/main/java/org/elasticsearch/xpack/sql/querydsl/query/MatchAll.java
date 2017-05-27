/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class MatchAll extends Query {

    public MatchAll(Location location) {
        super(location, emptyList());
    }

    @Override
    public QueryBuilder asBuilder() {
        return matchAllQuery();
    }
}
