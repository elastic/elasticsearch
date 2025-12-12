/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.chunks;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.OffsetAndScore;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractContent;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractOffsetAndScores;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.getContentFromLegacyNestedSources;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.queries;

/**
 * Scores chunks from a {@link SemanticTextField}
 */
public class SemanticChunkScorer {

    private final SearchContext searchContext;

    public SemanticChunkScorer(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    /**
     * @param fieldType Semantic text field type
     * @param hit Search hit
     * @param inferenceText the query text to compare against
     * @param maxResults maximum number of results to return
     * @return list of scored chunks ordered by relevance
     * @throws IOException on failure scoring chunks
     */
    public List<ScoredChunk> scoreChunks(
        SemanticTextFieldMapper.SemanticTextFieldType fieldType,
        SearchHit hit,
        String inferenceText,
        int maxResults
    ) throws IOException {
        // TODO Determine if we should support backfilling here
        if (inferenceText == null || inferenceText.trim().isEmpty()) {
            return new ArrayList<>();
        }

        MinimalServiceSettings modelSettings = fieldType.getModelSettings();
        if (modelSettings == null) {
            // Null model settings mean that nothing was indexed yet, so we can short-circuit
            return new ArrayList<>();
        }

        List<Query> queries = queries(fieldType.getEmbeddingsField(), modelSettings.taskType(), searchContext.query());
        if (queries.isEmpty()) {
            return new ArrayList<>();
        }

        IndexSearcher searcher = searchContext.searcher();
        int docId = hit.docId();
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        int readerIndex = ReaderUtil.subIndex(docId, leaves);
        LeafReaderContext leafContext = leaves.get(readerIndex);

        List<ScoredChunk> scoredChunks = new ArrayList<>();
        if (leafContext != null) {
            LeafReader leafReader = leafContext.reader();
            int localDocId = docId - leafContext.docBase;

            List<OffsetAndScore> chunkOffsetsAndScores;
            try {
                chunkOffsetsAndScores = extractOffsetAndScores(
                    searchContext.getSearchExecutionContext(),
                    leafReader,
                    fieldType,
                    localDocId,
                    queries
                );
            } catch (IOException e) {
                throw new ElasticsearchException("failed to extract offset and scores", e);
            }

            if (chunkOffsetsAndScores.isEmpty()) {
                return List.of();
            }

            chunkOffsetsAndScores.sort(Comparator.comparingDouble(OffsetAndScore::score).reversed());

            int numChunks = Math.min(chunkOffsetsAndScores.size(), maxResults);

            if (fieldType.useLegacyFormat()) {
                // Handle legacy format - extract from nested sources
                List<Map<?, ?>> nestedSources = XContentMapValues.extractNestedSources(
                    fieldType.getChunksField().fullPath(),
                    hit.getSourceAsMap()
                );
                for (int j = 0; j < numChunks; j++) {
                    OffsetAndScore offsetAndScore = chunkOffsetsAndScores.get(j);
                    String content = getContentFromLegacyNestedSources(fieldType.name(), offsetAndScore, nestedSources);
                    scoredChunks.add(new ScoredChunk(content, offsetAndScore.score()));
                }
            } else {
                // Handle new format - extract using offsets
                for (int j = 0; j < numChunks; j++) {
                    OffsetAndScore offsetAndScore = chunkOffsetsAndScores.get(j);
                    String key = offsetAndScore.offset().field();
                    String content = extractContent(offsetAndScore, hit.field(key));
                    if (content != null) {
                        scoredChunks.add(new ScoredChunk(content, offsetAndScore.score()));
                    }
                }
            }
        }

        return scoredChunks;
    }
}
