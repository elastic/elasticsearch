/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
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
        RRFRankShardContext context = new RRFRankShardContext(null, 0, 5, 1);

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

        RRFRankShardResult result = context.combine(topDocs);
        assertEquals(2, result.queryCount);
        assertEquals(5, result.rrfRankDocs.length);

        RRFRankDoc expected = new RRFRankDoc(8, -1, 2);
        expected.rank = 1;
        expected.positions[0] = 7;
        expected.positions[1] = 0;
        expected.scores[0] = 3.0f;
        expected.scores[1] = 8.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[0]);

        expected = new RRFRankDoc(1, -1, 2);
        expected.rank = 2;
        expected.positions[0] = 0;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 10.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[1]);

        expected = new RRFRankDoc(9, -1, 2);
        expected.rank = 3;
        expected.positions[0] = 8;
        expected.positions[1] = 1;
        expected.scores[0] = 2.0f;
        expected.scores[1] = 7.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[2]);

        expected = new RRFRankDoc(10, -1, 2);
        expected.rank = 4;
        expected.positions[0] = 9;
        expected.positions[1] = 2;
        expected.scores[0] = 1.0f;
        expected.scores[1] = 6.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[3]);

        expected = new RRFRankDoc(2, -1, 2);
        expected.rank = 5;
        expected.positions[0] = 1;
        expected.positions[1] = NO_RANK;
        expected.scores[0] = 9.0f;
        expected.scores[1] = 0.0f;
        expected.score = Float.NaN;
        assertRDEquals(expected, result.rrfRankDocs[4]);
    }

    public void testCoordinatorRank() {
        RRFRankCoordinatorContext context = new RRFRankCoordinatorContext(3, 0, 5, 1);
        QuerySearchResult qsr0 = new QuerySearchResult();
        qsr0.setShardIndex(1);
        qsr0.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] {}));
        QuerySearchResult qsr1 = new QuerySearchResult();
        qsr1.setShardIndex(2);
        qsr1.setRankShardResult(new RRFRankShardResult(2, new RRFRankDoc[] {}));
        TopDocsStats tds = new TopDocsStats(0);
        SortedTopDocs std = context.rank(List.of(qsr0, qsr1), tds);
        std.scoreDocs();
    }
}
