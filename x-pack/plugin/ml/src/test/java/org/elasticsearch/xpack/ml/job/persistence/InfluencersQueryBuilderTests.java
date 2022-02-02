/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;

public class InfluencersQueryBuilderTests extends ESTestCase {

    public void testDefaultBuild() throws Exception {
        InfluencersQueryBuilder.InfluencersQuery query = new InfluencersQueryBuilder().build();

        assertEquals(0, query.getFrom());
        assertEquals(InfluencersQueryBuilder.DEFAULT_SIZE, query.getSize());
        assertEquals(false, query.isIncludeInterim());
        assertEquals(0.0, query.getInfluencerScoreFilter(), 0.0001);
        assertNull(query.getStart());
        assertNull(query.getEnd());
        assertEquals(Influencer.INFLUENCER_SCORE.getPreferredName(), query.getSortField());
        assertFalse(query.isSortDescending());
    }

    public void testAll() {
        InfluencersQueryBuilder.InfluencersQuery query = new InfluencersQueryBuilder().from(20)
            .size(40)
            .includeInterim(true)
            .influencerScoreThreshold(50.0d)
            .start("1000")
            .end("2000")
            .sortField("anomaly_score")
            .sortDescending(true)
            .build();

        assertEquals(20, query.getFrom());
        assertEquals(40, query.getSize());
        assertEquals(true, query.isIncludeInterim());
        assertEquals(50.0d, query.getInfluencerScoreFilter(), 0.00001);
        assertEquals("1000", query.getStart());
        assertEquals("2000", query.getEnd());
        assertEquals("anomaly_score", query.getSortField());
        assertTrue(query.isSortDescending());
    }

    public void testEqualsHash() {
        InfluencersQueryBuilder query = new InfluencersQueryBuilder().from(20)
            .size(40)
            .includeInterim(true)
            .influencerScoreThreshold(50.0d)
            .start("1000")
            .end("2000");

        InfluencersQueryBuilder query2 = new InfluencersQueryBuilder().from(20)
            .size(40)
            .includeInterim(true)
            .influencerScoreThreshold(50.0d)
            .start("1000")
            .end("2000");

        assertEquals(query.build(), query2.build());
        assertEquals(query.build().hashCode(), query2.build().hashCode());
        query2.clear();
        assertFalse(query.build().equals(query2.build()));

        query2.from(20).size(40).includeInterim(true).influencerScoreThreshold(50.0d).start("1000").end("2000");
        assertEquals(query.build(), query2.build());

        query2.clear();
        query2.from(20).size(40).includeInterim(true).influencerScoreThreshold(50.1d).start("1000").end("2000");
        assertFalse(query.build().equals(query2.build()));
    }
}
