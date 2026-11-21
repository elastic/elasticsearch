/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

public class PushMinMaxStatsToLocalSourceTests extends ESTestCase {

    public void testNoQueryReadsEveryDocument() {
        assertTrue(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(null, Set.of("a")));
        assertTrue(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(QueryBuilders.matchAllQuery(), Set.of("a")));
    }

    public void testExistsOnTheAggregatedFieldKeepsEveryDocumentWithAValue() {
        assertTrue(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(QueryBuilders.existsQuery("a").boost(0.0f), Set.of("a")));
    }

    public void testOrOfExistsKeepsEveryDocumentWithAValue() {
        var query = QueryBuilders.boolQuery().should(QueryBuilders.existsQuery("a")).should(QueryBuilders.existsQuery("b"));
        assertTrue(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(query, Set.of("a", "b")));
        assertTrue(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(query, Set.of("a")));
    }

    /** Documents that hold only a field the filter does not cover would be dropped, and the aggregate of that field reads them. */
    public void testExistsOnFewerFieldsThanAggregatedDropsDocuments() {
        assertFalse(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(QueryBuilders.existsQuery("a"), Set.of("a", "b")));
        var query = QueryBuilders.boolQuery().should(QueryBuilders.existsQuery("a")).should(QueryBuilders.existsQuery("c"));
        assertFalse(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(query, Set.of("a", "b")));
    }

    public void testAnyOtherQueryNarrowsTheDocuments() {
        assertFalse(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(QueryBuilders.termQuery("a", 1), Set.of("a")));
        assertFalse(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(QueryBuilders.rangeQuery("a").gt(1), Set.of("a")));
        assertFalse(
            PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(
                QueryBuilders.boolQuery().should(QueryBuilders.existsQuery("a")).should(QueryBuilders.termQuery("a", 1)),
                Set.of("a")
            )
        );
        // a must or filter clause narrows, even around an exists
        assertFalse(
            PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(
                QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("a")),
                Set.of("a")
            )
        );
        assertFalse(
            PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(
                QueryBuilders.boolQuery().should(QueryBuilders.existsQuery("a")).filter(QueryBuilders.termQuery("b", 1)),
                Set.of("a")
            )
        );
        assertFalse(PushMinMaxStatsToLocalSource.readsEveryDocumentWithAValue(new MatchNoneQueryBuilder(), Set.of("a")));
    }
}
