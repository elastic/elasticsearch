/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.fetch.subphase.ChunkResult;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.SemanticTextFieldType;
import org.elasticsearch.xpack.inference.queries.SemanticChunksExtBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.OffsetAndScore;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractOffsetAndScores;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractQueries;

/**
 * A {@link FetchSubPhase} that scores and returns individual semantic text chunks per hit
 * when a {@code semantic} query specifies {@code min_score} or {@code chunks_per_doc}.
 *
 * <p>Chunk configuration is read from {@link SemanticChunksExtBuilder} instances stored as
 * search ext on the search context. These are registered by {@code SemanticQueryBuilder}
 * during query rewrite, before it is replaced by a nested query. Chunk scoring reuses the
 * same Lucene-level logic as the semantic text highlighter.
 */
public class SemanticChunksFetchSubPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        List<SemanticChunksExtBuilder.ChunkConfig> configs = extractChunkConfigs(fetchContext);
        if (configs.isEmpty()) {
            return null;
        }

        SearchExecutionContext searchExecutionContext = fetchContext.getSearchExecutionContext();
        Query luceneQuery = fetchContext.query();

        // For each config, resolve the field type and extract the Lucene-level queries for scoring
        Map<SemanticChunksExtBuilder.ChunkConfig, FieldQueryPair> fieldQueries = new LinkedHashMap<>();
        for (SemanticChunksExtBuilder.ChunkConfig config : configs) {
            MappedFieldType mappedFieldType = searchExecutionContext.getFieldType(config.fieldName());
            if (mappedFieldType instanceof SemanticTextFieldType == false) {
                continue;
            }
            SemanticTextFieldType fieldType = (SemanticTextFieldType) mappedFieldType;
            if (fieldType.getModelSettings() == null || fieldType.getEmbeddingsField() == null) {
                continue;
            }
            List<Query> queries = extractQueries(fieldType.getEmbeddingsField(), fieldType.getModelSettings().taskType(), luceneQuery);
            if (queries.isEmpty()) {
                continue;
            }
            fieldQueries.put(config, new FieldQueryPair(fieldType, queries));
        }

        if (fieldQueries.isEmpty()) {
            return null;
        }

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(org.apache.lucene.index.LeafReaderContext readerContext) {
                // No per-segment state needed
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, List<ChunkResult>> allChunks = new HashMap<>();
                for (var entry : fieldQueries.entrySet()) {
                    SemanticChunksExtBuilder.ChunkConfig config = entry.getKey();
                    FieldQueryPair fqp = entry.getValue();
                    SemanticTextFieldType fieldType = fqp.fieldType();
                    List<Query> queries = fqp.queries();

                    List<OffsetAndScore> chunks = extractOffsetAndScores(
                        searchExecutionContext,
                        hitContext.reader(),
                        fieldType,
                        hitContext.docId(),
                        queries
                    );
                    if (chunks.isEmpty()) {
                        continue;
                    }

                    // Build content extractor
                    Function<OffsetAndScore, String> contentExtractor = buildContentExtractor(
                        fieldType,
                        hitContext,
                        searchExecutionContext
                    );

                    List<ChunkResult> results = filterAndSortChunks(chunks, contentExtractor, config.minScore(), config.chunksPerDoc());
                    if (results.isEmpty() == false) {
                        allChunks.put(config.fieldName(), results);
                    }
                }
                if (allChunks.isEmpty() == false) {
                    hitContext.hit().setChunks(allChunks);
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }
        };
    }

    /**
     * Filters chunks by {@code minScore}, sorts by score descending (ties broken by start offset ascending),
     * and caps to {@code chunksPerDoc}. Extracts chunk text via the provided content extractor.
     */
    static List<ChunkResult> filterAndSortChunks(
        List<OffsetAndScore> chunks,
        Function<OffsetAndScore, String> contentExtractor,
        Float minScore,
        Integer chunksPerDoc
    ) {
        List<OffsetAndScore> filtered;
        if (minScore != null) {
            filtered = new ArrayList<>();
            for (OffsetAndScore chunk : chunks) {
                if (chunk.score() >= minScore) {
                    filtered.add(chunk);
                }
            }
        } else {
            filtered = new ArrayList<>(chunks);
        }
        if (filtered.isEmpty()) {
            return List.of();
        }

        // Sort by score descending, ties broken by start offset ascending
        filtered.sort(
            Comparator.comparingDouble(OffsetAndScore::score)
                .reversed()
                .thenComparing(c -> c.offset() != null ? c.offset().start() : c.index())
        );

        int size = chunksPerDoc != null ? Math.min(filtered.size(), chunksPerDoc) : filtered.size();
        List<ChunkResult> results = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            OffsetAndScore chunk = filtered.get(i);
            String text = contentExtractor.apply(chunk);
            if (text != null) {
                int startOffset = chunk.offset() != null ? chunk.offset().start() : 0;
                int endOffset = chunk.offset() != null ? chunk.offset().end() : text.length();
                results.add(new ChunkResult(text, startOffset, endOffset, chunk.score()));
            }
        }
        return results;
    }

    /**
     * Extracts chunk configurations from the {@link SemanticChunksExtBuilder} stored as a
     * search ext on the search context. Registered during query rewrite by
     * {@code SemanticQueryBuilder} before it is rewritten away.
     */
    private static List<SemanticChunksExtBuilder.ChunkConfig> extractChunkConfigs(FetchContext fetchContext) {
        var ext = fetchContext.getSearchExt(SemanticChunksExtBuilder.NAME);
        if (ext instanceof SemanticChunksExtBuilder chunksExt) {
            return chunksExt.configs();
        }
        return List.of();
    }

    private static Function<OffsetAndScore, String> buildContentExtractor(
        SemanticTextFieldType fieldType,
        HitContext hitContext,
        SearchExecutionContext searchExecutionContext
    ) {
        Map<String, String> fieldToContent = new HashMap<>();
        return entry -> {
            if (entry.offset() == null) {
                // Legacy format — no offsets available
                return null;
            }
            String content = fieldToContent.computeIfAbsent(entry.offset().field(), key -> {
                try {
                    return extractFieldContent(searchExecutionContext, hitContext, key);
                } catch (IOException e) {
                    throw new UncheckedIOException("Error extracting field content from field " + key, e);
                }
            });
            if (content == null) {
                return null;
            }
            return content.substring(entry.offset().start(), entry.offset().end());
        };
    }

    private static String extractFieldContent(SearchExecutionContext searchContext, HitContext hitContext, String sourceField)
        throws IOException {
        var sourceFieldType = searchContext.getMappingLookup().getFieldType(sourceField);
        if (sourceFieldType == null) {
            return null;
        }

        var values = HighlightUtils.loadFieldValues(sourceFieldType, searchContext, hitContext)
            .stream()
            .<Object>map((s) -> DefaultHighlighter.convertFieldValue(sourceFieldType, s))
            .toList();
        if (values.isEmpty()) {
            return null;
        }
        return DefaultHighlighter.mergeFieldValues(values, MULTIVAL_SEP_CHAR);
    }

    private record FieldQueryPair(SemanticTextFieldType fieldType, List<Query> queries) {}
}
