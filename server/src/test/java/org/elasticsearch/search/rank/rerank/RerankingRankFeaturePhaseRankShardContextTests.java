/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class RerankingRankFeaturePhaseRankShardContextTests extends ESTestCase {

    public void testBuildRankFeatureShardResult() {
        SearchHit hit0 = new SearchHit(0);
        hit0.score(1.0f);
        hit0.setDocumentField("some-field", new DocumentField("some-field", List.of("doc0-text")));
        SearchHit hit1 = new SearchHit(1);
        hit1.score(2.0f);
        hit1.setDocumentField("some-field", new DocumentField("some-field", List.of("doc1-text")));
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit0, hit1 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 10.0f);

        RerankingRankFeaturePhaseRankShardContext subject = new RerankingRankFeaturePhaseRankShardContext("some-field");
        RankFeatureShardResult rankShardResult = (RankFeatureShardResult) subject.buildRankFeatureShardResult(searchHits, 0);

        // verify the feature field is extracted and set in the results along with IDs and score
        assertEquals(2, rankShardResult.rankFeatureDocs.length);
        assertEquals("doc0-text", rankShardResult.rankFeatureDocs[0].featureData);
        assertEquals(0, rankShardResult.rankFeatureDocs[0].doc);
        assertEquals(1.0f, rankShardResult.rankFeatureDocs[0].score, 0.0f);
        assertEquals(0, rankShardResult.rankFeatureDocs[0].shardIndex);
        assertEquals("doc1-text", rankShardResult.rankFeatureDocs[1].featureData);
        assertEquals(1, rankShardResult.rankFeatureDocs[1].doc);
        assertEquals(2.0f, rankShardResult.rankFeatureDocs[1].score, 0.0f);
        assertEquals(0, rankShardResult.rankFeatureDocs[1].shardIndex);
    }

}
