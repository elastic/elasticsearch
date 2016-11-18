/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.test.ESTestCase;

public class BucketsQueryBuilderTests extends ESTestCase {

    public void testDefaultBuild() throws Exception {
        BucketsQueryBuilder.BucketsQuery query = new BucketsQueryBuilder().build();

        assertEquals(0, query.getFrom());
        assertEquals(BucketsQueryBuilder.DEFAULT_SIZE, query.getSize());
        assertEquals(false, query.isIncludeInterim());
        assertEquals(false, query.isExpand());
        assertEquals(0.0, query.getAnomalyScoreFilter(), 0.0001);
        assertEquals(0.0, query.getNormalizedProbability(), 0.0001);
        assertNull(query.getEpochStart());
        assertNull(query.getEpochEnd());
        assertEquals("timestamp", query.getSortField());
        assertFalse(query.isSortDescending());
    }

    public void testAll() {
        BucketsQueryBuilder.BucketsQuery query = new BucketsQueryBuilder()
                .from(20)
                .size(40)
                .includeInterim(true)
                .expand(true)
                .anomalyScoreThreshold(50.0d)
                .normalizedProbabilityThreshold(70.0d)
                .epochStart("1000")
                .epochEnd("2000")
                .partitionValue("foo")
                .sortField("anomalyScore")
                .sortDescending(true)
                .build();

        assertEquals(20, query.getFrom());
        assertEquals(40, query.getSize());
        assertEquals(true, query.isIncludeInterim());
        assertEquals(true, query.isExpand());
        assertEquals(50.0d, query.getAnomalyScoreFilter(), 0.00001);
        assertEquals(70.0d, query.getNormalizedProbability(), 0.00001);
        assertEquals("1000", query.getEpochStart());
        assertEquals("2000", query.getEpochEnd());
        assertEquals("foo", query.getPartitionValue());
        assertEquals("anomalyScore", query.getSortField());
        assertTrue(query.isSortDescending());
    }

    public void testEqualsHash() {
        BucketsQueryBuilder query = new BucketsQueryBuilder()
                .from(20)
                .size(40)
                .includeInterim(true)
                .expand(true)
                .anomalyScoreThreshold(50.0d)
                .normalizedProbabilityThreshold(70.0d)
                .epochStart("1000")
                .epochEnd("2000")
                .partitionValue("foo");

        BucketsQueryBuilder query2 = new BucketsQueryBuilder()
                .from(20)
                .size(40)
                .includeInterim(true)
                .expand(true)
                .anomalyScoreThreshold(50.0d)
                .normalizedProbabilityThreshold(70.0d)
                .epochStart("1000")
                .epochEnd("2000")
                .partitionValue("foo");

        assertEquals(query.build(), query2.build());
        assertEquals(query.build().hashCode(), query2.build().hashCode());
        query2.clear();
        assertFalse(query.build().equals(query2.build()));

        query2.from(20)
        .size(40)
        .includeInterim(true)
        .expand(true)
        .anomalyScoreThreshold(50.0d)
        .normalizedProbabilityThreshold(70.0d)
        .epochStart("1000")
        .epochEnd("2000")
        .partitionValue("foo");
        assertEquals(query.build(), query2.build());

        query2.clear();
        query2.from(20)
        .size(40)
        .includeInterim(true)
        .expand(true)
        .anomalyScoreThreshold(50.1d)
        .normalizedProbabilityThreshold(70.0d)
        .epochStart("1000")
        .epochEnd("2000")
        .partitionValue("foo");
        assertFalse(query.build().equals(query2.build()));
    }
}