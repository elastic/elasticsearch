/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;

import java.util.Arrays;

/**
 * The {@code ReRankingRankFeaturePhaseRankShardContext} is handles the {@code SearchHits} generated from the {@code RankFeatureShardPhase}
 * and builds the {@code RankFeatureShardResult} for the reranking phase, by reading the field info for the specified {@code field} during
 * construction.
 */
public class RerankingRankFeaturePhaseRankShardContext extends RankFeaturePhaseRankShardContext {

    private static final Logger logger = LogManager.getLogger(RerankingRankFeaturePhaseRankShardContext.class);

    public RerankingRankFeaturePhaseRankShardContext(String field) {
        super(field);
    }

    @Override
    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
        try {
            RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
            for (int i = 0; i < hits.getHits().length; i++) {
                rankFeatureDocs[i] = new RankFeatureDoc(hits.getHits()[i].docId(), hits.getHits()[i].getScore(), shardId);
                DocumentField docField = hits.getHits()[i].field(field);
                if (docField != null) {
                    rankFeatureDocs[i].featureData(docField.getValue().toString());
                }
            }
            return new RankFeatureShardResult(rankFeatureDocs);
        } catch (Exception ex) {
            logger.warn(
                "Error while fetching feature data for {field: ["
                    + field
                    + "]} and {docids: ["
                    + Arrays.stream(hits.getHits()).map(SearchHit::docId).toList()
                    + "]}.",
                ex
            );
            return null;
        }
    }
}
