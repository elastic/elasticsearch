/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PrefilteringTestUtils {

    public static void setRandomTermQueryPrefilters(PrefilteredQuery<?> queryBuilder, String... termFieldNames) {
        List<QueryBuilder> filters = new ArrayList<>();
        int numFilters = randomIntBetween(1, 5);
        for (int i = 0; i < numFilters; i++) {
            String filterFieldName = randomFrom(termFieldNames);
            filters.add(QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10)));
        }
        queryBuilder.setPrefilters(filters);
    }

    public static void assertQueryHasPrefilters(PrefilteredQuery<?> queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        assertThat(query, anyOf(instanceOf(KnnFloatVectorQuery.class), instanceOf(KnnByteVectorQuery.class)));
        Query queryFilter;
        if (query instanceof KnnFloatVectorQuery q) {
            queryFilter = q.getFilter();
        } else if (query instanceof KnnByteVectorQuery q) {
            queryFilter = q.getFilter();
        } else {
            throw new IllegalStateException("Unexpected query type " + query.getClass());
        }

        if (queryBuilder.getPrefilters().isEmpty()) {
            assertThat(queryFilter, is(nullValue()));
        } else {
            for (QueryBuilder qb : queryBuilder.getPrefilters()) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(qb.toQuery(context), BooleanClause.Occur.MUST);
                BooleanQuery booleanQuery = builder.build();
                assertThat(queryFilter.toString(), containsString(booleanQuery.toString()));
            }
        }
    }
}
