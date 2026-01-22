/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.search.rank.RankDoc.NO_RANK;

public class RRFRankContextTests extends ESTestCase {

    private void assertRDEquals(RRFRankDoc rd0, RRFRankDoc rd1) {
        assertEquals(rd0.doc, rd1.doc);
        assertEquals(rd0.score, rd1.score, 0.01);
        assertEquals(rd0.shardIndex, rd1.shardIndex);
        assertEquals(rd0.rank, rd1.rank);
        assertEquals(rd0.positions.length, rd1.positions.length);
        assertEquals(rd0.scores.length, rd1.scores.length);
        assertEquals(rd1.positions.length, rd1.scores.length);

        for (int i = 0; i < rd0.positions.length; ++i) {
            assertEquals(rd0.positions[i], rd1.positions[i]);
            assertEquals(rd0.scores[i], rd1.scores[i], 0.01);
        }
    }

    public void testShardCombine() {
        RRFQueryPhaseRankShardContext context = new RRFQueryPhaseRankShardContext(null, 10, 1);
        List<TopDocs> topDocs = List.of(
            new TopDocs(
                null,
                new ScoreDoc[] {
                    new ScoreDoc(1, 10.0f, -1), // 0.500
                    new ScoreDoc(2, 9.0f, -1),  // 0.333
                    new ScoreDoc(3, 8.0f, -1),  // 0.250
                    new ScoreDoc(4, 7.0f, -1),  // 0.200
                    new ScoreDoc(5, 6.0f, -1),  // 0.167
                    new ScoreDoc(6, 5.0f, -1),  // 0.143
                    new ScoreDoc(7, 4.0f, -1),  // 0.125
                    new ScoreDoc(8, 3.0f, -1),  // 0.111
                    new ScoreDoc(9, 2.0f, -1),  // 0.100
                    new ScoreDoc(10, 1.0f, -1), // 0.091
                }
            ),
            new TopDocs(
                null,
                new ScoreDoc[] {
                    new ScoreDoc(8, 8.0f, -1),  // 0.500
                    new ScoreDoc(9, 7.0f, -1),  // 0.333
                    new ScoreDoc(10, 6.0f, -1), // 0.250
                    new ScoreDoc(11, 5.0f, -1), // 0.200
                    new ScoreDoc(12, 4.0f, -1), // 0.167
                    new ScoreDoc(13, 3.0f, -1), // 0.143
                    new ScoreDoc(14, 2.0f, -1), // 0.125
                    new ScoreDoc(15, 1.0f, -1), // 0.111
                }
            )
        );

        RRFRankShardResult result = context.combineQueryPhaseResults(topDocs);
        assertEquals(2, result.queryCount);
        assertEquals(10, result.rrfRankDocs.length);

        RRFRankDoc expected = new RRFRankDoc(8, -1, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 7;
        expected.positions[1] = 0;
        expected.scores[0] = 3.0f;
        expected.scores[1] = 8.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[0]);

        expected = new RRFRankDoc(1, -1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = 0;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[1]);

        expected = new RRFRankDoc(9, -1, 2, context.rankConstant());
        expected.rank = 3;
        expected.positions[0] = 8;
        expected.positions[1] = 1;
        expected.scores[0] = 2.0f;
        expected.scores[1] = 7.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[2]);

        expected = new RRFRankDoc(10, -1, 2, context.rankConstant());
        expected.rank = 4;
        expected.positions[0] = 9;
        expected.positions[1] = 2;
        expected.scores[0] = 1.0f;
        expected.scores[1] = 6.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[3]);

        expected = new RRFRankDoc(2, -1, 2, context.rankConstant());
        expected.rank = 5;
        expected.positions[0] = 1;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[4]);

        expected = new RRFRankDoc(3, -1, 2, context.rankConstant());
        expected.rank = 6;
        expected.positions[0] = 2;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 8.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[5]);

        expected = new RRFRankDoc(4, -1, 2, context.rankConstant());
        expected.rank = 7;
        expected.positions[0] = 3;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 7.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[6]);

        expected = new RRFRankDoc(11, -1, 2, context.rankConstant());
        expected.rank = 8;
        expected.positions[0] = NO_RANK;
        expected.positions[1] = 3;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 5.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[7]);

        expected = new RRFRankDoc(5, -1, 2, context.rankConstant());
        expected.rank = 9;
        expected.positions[0] = 4;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 6.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[8]);

        expected = new RRFRankDoc(12, -1, 2, context.rankConstant());
        expected.rank = 10;
        expected.positions[0] = NO_RANK;
        expected.positions[1] = 4;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 4.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[9]);
    }

    public void testCoordinatorRank() {
        RRFQueryPhaseRankCoordinatorContext context = new RRFQueryPhaseRankCoordinatorContext(4, 0, 5, 1);
        QuerySearchResult qsr0 = new QuerySearchResult();
        qsr0.setShardIndex(1);
        RRFRankDoc rd11 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd11.positions[0] = 2;
        rd11.positions[1] = 0;
        rd11.scores[0] = 3.0f;
        rd11.scores[1] = 8.0f;
        RRFRankDoc rd12 = new RRFRankDoc(2, -1, 2, context.rankConstant());
        rd12.positions[0] = 3;
        rd12.positions[1] = 1;
        rd12.scores[0] = 2.0f;
        rd12.scores[1] = 7.0f;
        RRFRankDoc rd13 = new RRFRankDoc(3, -1, 2, context.rankConstant());
        rd13.positions[0] = 0;
        rd13.positions[1] = NO_RANK;
        rd13.scores[0] = 10.0f;
        rd13.scores[1] = 0.0f;
        RRFRankDoc rd14 = new RRFRankDoc(4, -1, 2, context.rankConstant());
        rd14.positions[0] = 4;
        rd14.positions[1] = 2;
        rd14.scores[0] = 1.0f;
        rd14.scores[1] = 6.0f;
        RRFRankDoc rd15 = new RRFRankDoc(5, -1, 2, context.rankConstant());
        rd15.positions[0] = 1;
        rd15.positions[1] = NO_RANK;
        rd15.scores[0] = 9.0f;
        rd15.scores[1] = 0.0f;
        qsr0.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd11, rd12, rd13, rd14, rd15 }));

        QuerySearchResult qsr1 = new QuerySearchResult();
        qsr1.setShardIndex(2);
        RRFRankDoc rd21 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd21.positions[0] = 0;
        rd21.positions[1] = 0;
        rd21.scores[0] = 9.5f;
        rd21.scores[1] = 7.5f;
        RRFRankDoc rd22 = new RRFRankDoc(2, -1, 2, context.rankConstant());
        rd22.positions[0] = 1;
        rd22.positions[1] = 1;
        rd22.scores[0] = 8.5f;
        rd22.scores[1] = 6.5f;
        RRFRankDoc rd23 = new RRFRankDoc(3, -1, 2, context.rankConstant());
        rd23.positions[0] = 2;
        rd23.positions[1] = 2;
        rd23.scores[0] = 7.5f;
        rd23.scores[1] = 4.5f;
        RRFRankDoc rd24 = new RRFRankDoc(4, -1, 2, context.rankConstant());
        rd24.positions[0] = 3;
        rd24.positions[1] = NO_RANK;
        rd24.scores[0] = 5.5f;
        rd24.scores[1] = 0.0f;
        RRFRankDoc rd25 = new RRFRankDoc(5, -1, 2, context.rankConstant());
        rd25.positions[0] = NO_RANK;
        rd25.positions[1] = 3;
        rd25.scores[0] = 0.0f;
        rd25.scores[1] = 4.5f;
        qsr1.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd21, rd22, rd23, rd24, rd25 }));

        TopDocsStats tds = new TopDocsStats(0);
        ScoreDoc[] scoreDocs = context.rankQueryPhaseResults(List.of(qsr0, qsr1), tds);

        assertEquals(4, tds.fetchHits);
        assertEquals(4, scoreDocs.length);

        RRFRankDoc expected = new RRFRankDoc(1, 2, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 1;
        expected.positions[1] = 1;
        expected.scores[0] = 9.5f;
        expected.scores[1] = 7.5f;
        expected.score = 0.6666667f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[0]);

        expected = new RRFRankDoc(3, 1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = 0;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = 0.5f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[1]);

        expected = new RRFRankDoc(1, 1, 2, context.rankConstant());
        expected.rank = 3;
        expected.positions[0] = NO_RANK;
        expected.positions[1] = 0;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 8.0f;
        expected.score = 0.5f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[2]);

        expected = new RRFRankDoc(2, 2, 2, context.rankConstant());
        expected.rank = 4;
        expected.positions[0] = 3;
        expected.positions[1] = 3;
        expected.scores[0] = 8.5f;
        expected.scores[1] = 6.5f;
        expected.score = 0.4f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[3]);
    }

    public void testShardTieBreaker() {
        RRFQueryPhaseRankShardContext context = new RRFQueryPhaseRankShardContext(null, 10, 1);

        List<TopDocs> topDocs = List.of(
            new TopDocs(null, new ScoreDoc[] { new ScoreDoc(1, 10.0f, -1), new ScoreDoc(2, 9.0f, -1) }),
            new TopDocs(null, new ScoreDoc[] { new ScoreDoc(2, 8.0f, -1), new ScoreDoc(1, 7.0f, -1) })
        );

        RRFRankShardResult result = context.combineQueryPhaseResults(topDocs);
        assertEquals(2, result.queryCount);
        assertEquals(2, result.rrfRankDocs.length);

        RRFRankDoc expected = new RRFRankDoc(1, -1, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 0;
        expected.positions[1] = 1;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 7.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[0]);

        expected = new RRFRankDoc(2, -1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = 1;
        expected.positions[1] = 0;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 8.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[1]);

        topDocs = List.of(
            new TopDocs(null, new ScoreDoc[] { new ScoreDoc(1, 10.0f, -1), new ScoreDoc(2, 9.0f, -1), new ScoreDoc(3, 9.0f, -1) }),
            new TopDocs(null, new ScoreDoc[] { new ScoreDoc(4, 11.0f, -1), new ScoreDoc(3, 9.0f, -1), new ScoreDoc(2, 7.0f, -1) })
        );

        result = context.combineQueryPhaseResults(topDocs);
        assertEquals(2, result.queryCount);
        assertEquals(4, result.rrfRankDocs.length);

        expected = new RRFRankDoc(3, -1, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 2;
        expected.positions[1] = 1;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 9.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[0]);

        expected = new RRFRankDoc(2, -1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = 1;
        expected.positions[1] = 2;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 7.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[1]);

        expected = new RRFRankDoc(1, -1, 2, context.rankConstant());
        expected.rank = 3;
        expected.positions[0] = 0;
        expected.positions[1] = -1;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[2]);

        expected = new RRFRankDoc(4, -1, 2, context.rankConstant());
        expected.rank = 4;
        expected.positions[0] = -1;
        expected.positions[1] = 0;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 11.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[3]);

        topDocs = List.of(
            new TopDocs(null, new ScoreDoc[] { new ScoreDoc(1, 10.0f, -1), new ScoreDoc(3, 3.0f, -1) }),
            new TopDocs(null, new ScoreDoc[] { new ScoreDoc(2, 8.0f, -1), new ScoreDoc(4, 5.0f, -1) })
        );

        result = context.combineQueryPhaseResults(topDocs);
        assertEquals(2, result.queryCount);
        assertEquals(4, result.rrfRankDocs.length);

        expected = new RRFRankDoc(1, -1, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 0;
        expected.positions[1] = -1;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[0]);

        expected = new RRFRankDoc(2, -1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = -1;
        expected.positions[1] = 0;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 8.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[1]);

        expected = new RRFRankDoc(3, -1, 2, context.rankConstant());
        expected.rank = 3;
        expected.positions[0] = 1;
        expected.positions[1] = -1;
        expected.scores[0] = 3.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[2]);

        expected = new RRFRankDoc(4, -1, 2, context.rankConstant());
        expected.rank = 4;
        expected.positions[0] = -1;
        expected.positions[1] = 1;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 5.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[3]);
    }

    public void testCoordinatorRankTieBreaker() {
        RRFQueryPhaseRankCoordinatorContext context = new RRFQueryPhaseRankCoordinatorContext(4, 0, 5, 1);

        QuerySearchResult qsr0 = new QuerySearchResult();
        qsr0.setShardIndex(1);
        RRFRankDoc rd11 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd11.positions[0] = 0;
        rd11.positions[1] = 0;
        rd11.scores[0] = 10.0f;
        rd11.scores[1] = 7.0f;
        qsr0.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd11 }));

        QuerySearchResult qsr1 = new QuerySearchResult();
        qsr1.setShardIndex(2);
        RRFRankDoc rd21 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd21.positions[0] = 0;
        rd21.positions[1] = 0;
        rd21.scores[0] = 9.0f;
        rd21.scores[1] = 8.0f;
        qsr1.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd21 }));

        TopDocsStats tds = new TopDocsStats(0);
        ScoreDoc[] scoreDocs = context.rankQueryPhaseResults(List.of(qsr0, qsr1), tds);

        assertEquals(2, tds.fetchHits);
        assertEquals(2, scoreDocs.length);

        RRFRankDoc expected = new RRFRankDoc(1, 1, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 0;
        expected.positions[1] = 1;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 7.0f;
        expected.score = 0.8333333730697632f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[0]);

        expected = new RRFRankDoc(1, 2, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = 1;
        expected.positions[1] = 0;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 8.0f;
        expected.score = 0.8333333730697632f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[1]);

        qsr0 = new QuerySearchResult();
        qsr0.setShardIndex(1);
        rd11 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd11.positions[0] = 0;
        rd11.positions[1] = -1;
        rd11.scores[0] = 10.0f;
        rd11.scores[1] = 0.0f;
        RRFRankDoc rd12 = new RRFRankDoc(2, -1, 2, context.rankConstant());
        rd12.positions[0] = 0;
        rd12.positions[1] = 1;
        rd12.scores[0] = 9.0f;
        rd12.scores[1] = 7.0f;
        qsr0.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd11, rd12 }));

        qsr1 = new QuerySearchResult();
        qsr1.setShardIndex(2);
        rd21 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd21.positions[0] = -1;
        rd21.positions[1] = 0;
        rd21.scores[0] = 0.0f;
        rd21.scores[1] = 11.0f;
        RRFRankDoc rd22 = new RRFRankDoc(2, -1, 2, context.rankConstant());
        rd22.positions[0] = 0;
        rd22.positions[1] = 1;
        rd22.scores[0] = 9.0f;
        rd22.scores[1] = 9.0f;
        qsr1.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd21, rd22 }));

        tds = new TopDocsStats(0);
        scoreDocs = context.rankQueryPhaseResults(List.of(qsr0, qsr1), tds);

        assertEquals(4, tds.fetchHits);
        assertEquals(4, scoreDocs.length);

        expected = new RRFRankDoc(2, 2, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 2;
        expected.positions[1] = 1;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 9.0f;
        expected.score = 0.5833333730697632f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[0]);

        expected = new RRFRankDoc(2, 1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = 1;
        expected.positions[1] = 2;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 7.0f;
        expected.score = 0.5833333730697632f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[1]);

        expected = new RRFRankDoc(1, 1, 2, context.rankConstant());
        expected.rank = 3;
        expected.positions[0] = 0;
        expected.positions[1] = -1;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = 0.5f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[2]);

        expected = new RRFRankDoc(1, 2, 2, context.rankConstant());
        expected.rank = 4;
        expected.positions[0] = -1;
        expected.positions[1] = 0;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 11.0f;
        expected.score = 0.5f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[3]);

        qsr0 = new QuerySearchResult();
        qsr0.setShardIndex(1);
        rd11 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd11.positions[0] = 0;
        rd11.positions[1] = -1;
        rd11.scores[0] = 10.0f;
        rd11.scores[1] = 0.0f;
        rd12 = new RRFRankDoc(2, -1, 2, context.rankConstant());
        rd12.positions[0] = -1;
        rd12.positions[1] = 0;
        rd12.scores[0] = 0.0f;
        rd12.scores[1] = 12.0f;
        qsr0.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd11, rd12 }));

        qsr1 = new QuerySearchResult();
        qsr1.setShardIndex(2);
        rd21 = new RRFRankDoc(1, -1, 2, context.rankConstant());
        rd21.positions[0] = 0;
        rd21.positions[1] = -1;
        rd21.scores[0] = 3.0f;
        rd21.scores[1] = 0.0f;
        rd22 = new RRFRankDoc(2, -1, 2, context.rankConstant());
        rd22.positions[0] = -1;
        rd22.positions[1] = 0;
        rd22.scores[0] = 0.0f;
        rd22.scores[1] = 5.0f;
        qsr1.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] { rd21, rd22 }));

        tds = new TopDocsStats(0);
        scoreDocs = context.rankQueryPhaseResults(List.of(qsr0, qsr1), tds);

        assertEquals(4, tds.fetchHits);
        assertEquals(4, scoreDocs.length);

        expected = new RRFRankDoc(1, 1, 2, context.rankConstant());
        expected.rank = 1;
        expected.positions[0] = 0;
        expected.positions[1] = -1;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = 0.5f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[0]);

        expected = new RRFRankDoc(2, 1, 2, context.rankConstant());
        expected.rank = 2;
        expected.positions[0] = -1;
        expected.positions[1] = 0;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 12.0f;
        expected.score = 0.5f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[1]);

        expected = new RRFRankDoc(1, 2, 2, context.rankConstant());
        expected.rank = 3;
        expected.positions[0] = 1;
        expected.positions[1] = -1;
        expected.scores[0] = 3.0f;
        expected.scores[1] = 0.0f;
        expected.score = 0.3333333333333333f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[2]);

        expected = new RRFRankDoc(2, 2, 2, context.rankConstant());
        expected.rank = 4;
        expected.positions[0] = -1;
        expected.positions[1] = 1;
        expected.scores[0] = 0.0f;
        expected.scores[1] = 5.0f;
        expected.score = 0.3333333333333333f;
        assertRDEquals(expected, (RRFRankDoc) scoreDocs[3]);
    }
}
