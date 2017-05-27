/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import java.util.Arrays;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.tree.Location;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class OrQuery extends Query {

    private final Query left, right;

    public OrQuery(Location location, Query left, Query right) {
        super(location, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    public Query left() {
        return left;
    }

    public Query right() {
        return right;
    }

    @Override
    public QueryBuilder asBuilder() {
        BoolQueryBuilder boolQuery = boolQuery();
        if (left != null) {
            boolQuery.should(left.asBuilder());
        }
        if (right != null) {
            boolQuery.should(right.asBuilder());
        }
        return boolQuery;
    }
}
