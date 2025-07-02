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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.search.rank.feature.RerankSnippetInput;
import org.elasticsearch.xcontent.Text;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The {@code ReRankingRankFeaturePhaseRankShardContext} is handles the {@code SearchHits} generated from the {@code RankFeatureShardPhase}
 * and builds the {@code RankFeatureShardResult} for the reranking phase, by reading the field info for the specified {@code field} during
 * construction.
 */
public class RerankingRankFeaturePhaseRankShardContext extends RankFeaturePhaseRankShardContext {

    private static final Logger logger = LogManager.getLogger(RerankingRankFeaturePhaseRankShardContext.class);
    private final RerankSnippetInput snippets;

    public RerankingRankFeaturePhaseRankShardContext(String field) {
        this(field, null);
    }

    public RerankingRankFeaturePhaseRankShardContext(String field, RerankSnippetInput snippets) {
        super(field);
        this.snippets = snippets;
    }

    @Override
    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
        try {
            RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
            for (int i = 0; i < hits.getHits().length; i++) {
                rankFeatureDocs[i] = new RankFeatureDoc(hits.getHits()[i].docId(), hits.getHits()[i].getScore(), shardId);
                SearchHit hit = hits.getHits()[i];
                DocumentField docField = hit.field(field);
                if (docField != null && snippets == null) {
                    rankFeatureDocs[i].featureData(List.of(docField.getValue().toString()));
                } else {
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    if (highlightFields != null && highlightFields.containsKey(field)) {
                        List<String> snippets = Arrays.stream(highlightFields.get(field).fragments()).map(Text::string).toList();
                        rankFeatureDocs[i].featureData(snippets);
                    } else if (docField != null) {
                        // If we did not get highlighting results, backfill with the doc field value
                        // but pass in a warning because we are not reranking on snippets only
                        rankFeatureDocs[i].featureData(List.of(docField.getValue().toString()));
                        HeaderWarning.addWarning(
                            "Reranking on snippets requested, but no snippets were found for field ["
                                + field
                                + "]. Using field value instead."
                        );
                    }
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
