/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SparseVectorFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceField;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.SemanticTextFieldType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

/**
 * A {@link Highlighter} designed for the {@link SemanticTextFieldMapper}.
 * This highlighter extracts semantic queries and evaluates them against each chunk produced by the semantic text field.
 * It returns the top-scoring chunks as snippets, optionally sorted by their scores.
 */
public class SemanticTextHighlighter implements Highlighter {
    public static final String NAME = "semantic";

    private static final String CHUNK_DELIMITER = " ";

    private record OffsetAndScore(int index, OffsetSourceFieldMapper.OffsetSource offset, float score) {}

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return fieldType instanceof SemanticTextFieldType;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        if (canHighlight(fieldContext.fieldType) == false) {
            return null;
        }
        SemanticTextFieldType fieldType = (SemanticTextFieldType) fieldContext.fieldType;
        if (fieldType.getModelSettings() == null || fieldType.getEmbeddingsField() == null) {
            // nothing indexed yet
            return null;
        }

        final List<Query> queries = switch (fieldType.getModelSettings().taskType()) {
            case SPARSE_EMBEDDING -> extractSparseVectorQueries(
                (SparseVectorFieldType) fieldType.getEmbeddingsField().fieldType(),
                fieldContext.query
            );
            case TEXT_EMBEDDING -> extractDenseVectorQueries(
                (DenseVectorFieldType) fieldType.getEmbeddingsField().fieldType(),
                fieldContext.query
            );
            default -> throw new IllegalStateException(
                "Wrong task type for a semantic text field, got [" + fieldType.getModelSettings().taskType().name() + "]"
            );
        };
        if (queries.isEmpty()) {
            // nothing to highlight
            return null;
        }

        int numberOfFragments = fieldContext.field.fieldOptions().numberOfFragments() <= 0
            ? 1 // we return the best fragment by default
            : fieldContext.field.fieldOptions().numberOfFragments();

        // TODO: Right now this will default to 100 if not set. If the user does not set fragmentSize for
        // the semantic highlighter, we probably just want to return unadulterated chunks instead.
        int fragmentCharSize = fieldContext.field.fieldOptions().fragmentCharSize();

        List<OffsetAndScore> chunks = extractOffsetAndScores(
            fieldContext.context.getSearchExecutionContext(),
            fieldContext.hitContext.reader(),
            fieldType,
            fieldContext.hitContext.docId(),
            queries
        );
        if (chunks.isEmpty()) {
            return null;
        }

        chunks.sort(Comparator.comparingDouble(OffsetAndScore::score).reversed());
        int maxSnippets = Math.min(chunks.size(), numberOfFragments);
        if (fieldContext.field.fieldOptions().scoreOrdered() == false) {
            chunks = chunks.subList(0, maxSnippets);
            chunks.sort(Comparator.comparingInt(c -> c.index));
        }
        List<Text> snippetsList = new ArrayList<>();
        final Function<OffsetAndScore, String> offsetToContent;
        if (fieldType.useLegacyFormat()) {
            List<Map<?, ?>> nestedSources = XContentMapValues.extractNestedSources(
                fieldType.getChunksField().fullPath(),
                fieldContext.hitContext.source().source()
            );
            offsetToContent = entry -> getContentFromLegacyNestedSources(fieldType.name(), entry, nestedSources);
        } else {
            Map<String, String> fieldToContent = new HashMap<>();
            offsetToContent = entry -> {
                String content = fieldToContent.computeIfAbsent(entry.offset().field(), key -> {
                    try {
                        return extractFieldContent(
                            fieldContext.context.getSearchExecutionContext(),
                            fieldContext.hitContext,
                            entry.offset.field()
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException("Error extracting field content from field " + entry.offset.field(), e);
                    }
                });
                return content.substring(entry.offset().start(), entry.offset().end());
            };
        }
        // Track which chunks have already been consumed to avoid duplication
        boolean[] consumedChunks = new boolean[chunks.size()];
        for (int i = 0; i < chunks.size() && snippetsList.size() < maxSnippets; i++) {
            if (consumedChunks[i]) {
                continue;
            }

            var chunk = chunks.get(i);
            String content = offsetToContent.apply(chunk);
            if (content == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,

                        "Invalid content detected for field [%s]: missing text for the chunk at offset [%s].",
                        fieldType.name(),
                        chunk.offset
                    )
                );
            }
            consumedChunks[i] = true;

            if (fragmentCharSize > 0 && content.length() < fragmentCharSize) {
                var cur = chunk.offset();
                if (cur != null) {
                    int prevIdx = -1, nextIdx = -1;
                    long bestPrevEnd = Long.MIN_VALUE;   // nearest previous by largest end()
                    long bestNextStart = Long.MAX_VALUE; // nearest next by smallest start()

                    for (int j = 0; j < chunks.size(); j++) {
                        if (j == i || consumedChunks[j]) continue;

                        var cand = chunks.get(j);
                        var off = cand.offset();
                        if (off == null) continue;

                        if (off.end() <= cur.start()) {
                            // candidate is before current
                            if (off.end() > bestPrevEnd) {
                                bestPrevEnd = off.end();
                                prevIdx = j;
                            }
                        } else if (off.start() >= cur.end()) {
                            // candidate is after current
                            if (off.start() < bestNextStart) {
                                bestNextStart = off.start();
                                nextIdx = j;
                            }
                        }
                    }

                    double prevScore = (prevIdx != -1) ? chunks.get(prevIdx).score() : Double.NEGATIVE_INFINITY;
                    double nextScore = (nextIdx != -1) ? chunks.get(nextIdx).score() : Double.NEGATIVE_INFINITY;

                    int pick = (nextScore > prevScore) ? nextIdx : prevIdx;
                    if (pick != -1) {
                        String extra = offsetToContent.apply(chunks.get(pick));
                        int remaining = fragmentCharSize - content.length() - CHUNK_DELIMITER.length();
                        if (extra != null) {
                            if (pick == nextIdx) {
                                String toAppend = extra.length() > remaining ? extra.substring(0, remaining) : extra;
                                content = content + CHUNK_DELIMITER + toAppend;        // expand forward
                            } else {
                                String toPrepend = extra.length() > remaining ? extra.substring(extra.length() - remaining) : extra;
                                content = toPrepend + CHUNK_DELIMITER + content;        // expand backward
                            }
                            consumedChunks[pick] = true;
                        }
                    }
                }
            }

            // Truncate content if it exceeds fragmentCharSize
            // TODO: This could potentially truncate the most relevant part of the snippet.
            if (fragmentCharSize > 0 && content.length() > fragmentCharSize) {
                content = content.substring(0, fragmentCharSize);
            }

            snippetsList.add(new Text(content));
        }

        // Convert list to array with the actual number of snippets produced
        Text[] snippets = snippetsList.toArray(new Text[0]);
        return new HighlightField(fieldContext.fieldName, snippets);
    }

    private String extractFieldContent(SearchExecutionContext searchContext, FetchSubPhase.HitContext hitContext, String sourceField)
        throws IOException {
        var sourceFieldType = searchContext.getMappingLookup().getFieldType(sourceField);
        if (sourceFieldType == null) {
            return null;
        }

        var values = HighlightUtils.loadFieldValues(sourceFieldType, searchContext, hitContext)
            .stream()
            .<Object>map((s) -> DefaultHighlighter.convertFieldValue(sourceFieldType, s))
            .toList();
        if (values.size() == 0) {
            return null;
        }
        return DefaultHighlighter.mergeFieldValues(values, MULTIVAL_SEP_CHAR);
    }

    private String getContentFromLegacyNestedSources(String fieldName, OffsetAndScore cand, List<Map<?, ?>> nestedSources) {
        if (nestedSources.size() <= cand.index) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Invalid content detected for field [%s]: the chunks size is [%d], "
                        + "but a reference to offset [%d] was found in the result.",
                    fieldName,
                    nestedSources.size(),
                    cand.index
                )
            );
        }
        return (String) nestedSources.get(cand.index).get(SemanticTextField.CHUNKED_TEXT_FIELD);
    }

    private List<OffsetAndScore> extractOffsetAndScores(
        SearchExecutionContext context,
        LeafReader reader,
        SemanticTextFieldType fieldType,
        int docId,
        List<Query> leafQueries
    ) throws IOException {
        var bitSet = context.bitsetFilter(fieldType.getChunksField().parentTypeFilter()).getBitSet(reader.getContext());
        int previousParent = docId > 0 ? bitSet.prevSetBit(docId - 1) : -1;

        BooleanQuery.Builder bq = new BooleanQuery.Builder().add(fieldType.getChunksField().nestedTypeFilter(), BooleanClause.Occur.FILTER);
        leafQueries.stream().forEach(q -> bq.add(q, BooleanClause.Occur.SHOULD));
        Weight weight = new IndexSearcher(reader).createWeight(bq.build(), ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.getContext());
        if (scorer == null) {
            return List.of();
        }
        if (previousParent != -1) {
            if (scorer.iterator().advance(previousParent) == DocIdSetIterator.NO_MORE_DOCS) {
                return List.of();
            }
        } else if (scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return List.of();
        }

        OffsetSourceField.OffsetSourceLoader offsetReader = null;
        if (fieldType.useLegacyFormat() == false) {
            var terms = reader.terms(fieldType.getOffsetsField().fullPath());
            if (terms == null) {
                // The field is empty
                return List.of();
            }
            offsetReader = OffsetSourceField.loader(terms);
        }

        List<OffsetAndScore> results = new ArrayList<>();
        int index = 0;
        while (scorer.docID() < docId) {
            if (offsetReader != null) {
                var offset = offsetReader.advanceTo(scorer.docID());
                if (offset == null) {
                    throw new IllegalStateException(
                        "Cannot highlight field [" + fieldType.name() + "], missing offsets for doc [" + docId + "]"
                    );
                }
                results.add(new OffsetAndScore(index++, offset, scorer.score()));
            } else {
                results.add(new OffsetAndScore(index++, null, scorer.score()));
            }
            if (scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
        }
        return results;
    }

    private List<Query> extractDenseVectorQueries(DenseVectorFieldType fieldType, Query querySection) {
        // TODO: Handle knn section when semantic text field can be used.
        List<Query> queries = new ArrayList<>();
        querySection.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return fieldType.name().equals(field);
            }

            @Override
            public void consumeTerms(Query query, Term... terms) {
                super.consumeTerms(query, terms);
            }

            private void visitLeaf(Query query, Float similarity) {
                if (query instanceof KnnFloatVectorQuery knnQuery) {
                    queries.add(fieldType.createExactKnnQuery(VectorData.fromFloats(knnQuery.getTargetCopy()), similarity));
                } else if (query instanceof KnnByteVectorQuery knnQuery) {
                    queries.add(fieldType.createExactKnnQuery(VectorData.fromBytes(knnQuery.getTargetCopy()), similarity));
                } else if (query instanceof MatchAllDocsQuery) {
                    queries.add(Queries.ALL_DOCS_INSTANCE);
                } else if (query instanceof DenseVectorQuery.Floats floatsQuery) {
                    queries.add(fieldType.createExactKnnQuery(VectorData.fromFloats(floatsQuery.getQuery()), similarity));
                } else if (query instanceof IVFKnnFloatVectorQuery ivfQuery) {
                    queries.add(fieldType.createExactKnnQuery(VectorData.fromFloats(ivfQuery.getQuery()), similarity));
                } else if (query instanceof RescoreKnnVectorQuery rescoreQuery) {
                    visitLeaf(rescoreQuery.innerQuery(), similarity);
                } else if (query instanceof VectorSimilarityQuery similarityQuery) {
                    visitLeaf(similarityQuery.getInnerKnnQuery(), similarityQuery.getSimilarity());
                }
            }

            @Override
            public void visitLeaf(Query query) {
                visitLeaf(query, null);
            }
        });
        return queries;
    }

    private List<Query> extractSparseVectorQueries(SparseVectorFieldType fieldType, Query querySection) {
        List<Query> queries = new ArrayList<>();
        querySection.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return fieldType.name().equals(field);
            }

            @Override
            public void consumeTerms(Query query, Term... terms) {
                super.consumeTerms(query, terms);
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                if (parent instanceof SparseVectorQueryWrapper sparseVectorQuery) {
                    queries.add(sparseVectorQuery.getTermsQuery());
                }
                return this;
            }

            @Override
            public void visitLeaf(Query query) {
                if (query instanceof MatchAllDocsQuery) {
                    queries.add(Queries.ALL_DOCS_INSTANCE);
                }
            }
        });
        return queries;
    }
}
