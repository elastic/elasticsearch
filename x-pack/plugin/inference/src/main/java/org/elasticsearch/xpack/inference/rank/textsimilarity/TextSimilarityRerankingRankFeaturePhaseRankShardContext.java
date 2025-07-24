/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.rank.textsimilarity.SnippetConfig.DEFAULT_NUM_SNIPPETS;

public class TextSimilarityRerankingRankFeaturePhaseRankShardContext extends RerankingRankFeaturePhaseRankShardContext {

    private final SnippetConfig snippetRankInput;

    // Rough approximation of token size vs. characters in highlight fragments.
    // TODO: highlighter should be able to set fragment size by token not length
    private static final int TOKEN_SIZE_LIMIT_MULTIPLIER = 5;

    public TextSimilarityRerankingRankFeaturePhaseRankShardContext(String field, @Nullable SnippetConfig snippetRankInput) {
        super(field);
        this.snippetRankInput = snippetRankInput;
    }

    @Override
    public RankShardResult doBuildRankFeatureShardResult(SearchHits hits, int shardId) {
        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
        for (int i = 0; i < hits.getHits().length; i++) {
            rankFeatureDocs[i] = new RankFeatureDoc(hits.getHits()[i].docId(), hits.getHits()[i].getScore(), shardId);
            SearchHit hit = hits.getHits()[i];
            DocumentField docField = hit.field(field);
            if (snippetRankInput == null && docField != null) {
                rankFeatureDocs[i].featureData(List.of(docField.getValue().toString()));
            } else {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                if (highlightFields != null && highlightFields.containsKey(field) && highlightFields.get(field).fragments().length > 0) {
                    List<String> snippets = Arrays.stream(highlightFields.get(field).fragments()).map(Text::string).toList();
                    rankFeatureDocs[i].featureData(snippets);
                } else if (docField != null) {
                    // If we did not get highlighting results, backfill with the doc field value
                    // but pass in a warning because we are not reranking on snippets only
                    rankFeatureDocs[i].featureData(List.of(docField.getValue().toString()));
                    HeaderWarning.addWarning(
                        "Reranking on snippets requested, but no snippets were found for field [" + field + "]. Using field value instead."
                    );
                }
            }
        }
        return new RankFeatureShardResult(rankFeatureDocs);
    }

    @Override
    public void prepareForFetch(SearchContext context) {
        if (snippetRankInput != null) {
            try {
                HighlightBuilder highlightBuilder = new HighlightBuilder();
                highlightBuilder.highlightQuery(snippetRankInput.snippetQueryBuilder());
                // Stripping pre/post tags as they're not useful for snippet creation
                highlightBuilder.field(field).preTags("").postTags("");
                // Return highest scoring fragments
                highlightBuilder.order(HighlightBuilder.Order.SCORE);
                int numSnippets = snippetRankInput.numSnippets() != null ? snippetRankInput.numSnippets() : DEFAULT_NUM_SNIPPETS;
                highlightBuilder.numOfFragments(numSnippets);
                // Rely on the model to determine the fragment size
                int tokenSizeLimit = snippetRankInput.tokenSizeLimit();
                int fragmentSize = tokenSizeLimit * TOKEN_SIZE_LIMIT_MULTIPLIER;
                highlightBuilder.fragmentSize(fragmentSize);
                highlightBuilder.noMatchSize(fragmentSize);
                SearchHighlightContext searchHighlightContext = highlightBuilder.build(context.getSearchExecutionContext());
                context.highlight(searchHighlightContext);
            } catch (IOException e) {
                throw new RuntimeException("Failed to generate snippet request", e);
            }
        }
    }

}
