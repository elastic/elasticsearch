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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SparseVectorFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryWrapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A {@link Highlighter} designed for the {@link SemanticTextFieldMapper}.
 * This highlighter extracts semantic queries and evaluates them against each chunk produced by the semantic text field.
 * It returns the top-scoring chunks as snippets, optionally sorted by their scores.
 */
public class SemanticTextHighlighter implements Highlighter {
    public static final String NAME = "semantic";

    private record OffsetAndScore(int offset, float score) {}

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            // TODO: Implement highlighting when using inference metadata fields
            return semanticTextFieldType.useLegacyFormat();
        }
        return false;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        SemanticTextFieldMapper.SemanticTextFieldType fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) fieldContext.fieldType;
        if (fieldType.getEmbeddingsField() == null) {
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

        List<OffsetAndScore> chunks = extractOffsetAndScores(
            fieldContext.context.getSearchExecutionContext(),
            fieldContext.hitContext.reader(),
            fieldType,
            fieldContext.hitContext.docId(),
            queries
        );
        if (chunks.size() == 0) {
            return null;
        }

        chunks.sort(Comparator.comparingDouble(OffsetAndScore::score).reversed());
        int size = Math.min(chunks.size(), numberOfFragments);
        if (fieldContext.field.fieldOptions().scoreOrdered() == false) {
            chunks = chunks.subList(0, size);
            chunks.sort(Comparator.comparingInt(c -> c.offset));
        }
        Text[] snippets = new Text[size];
        List<Map<?, ?>> nestedSources = XContentMapValues.extractNestedSources(
            fieldType.getChunksField().fullPath(),
            fieldContext.hitContext.source().source()
        );
        for (int i = 0; i < size; i++) {
            var chunk = chunks.get(i);
            if (nestedSources.size() <= chunk.offset) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "Invalid content detected for field [%s]: the chunks size is [%d], "
                            + "but a reference to offset [%d] was found in the result.",
                        fieldType.name(),
                        nestedSources.size(),
                        chunk.offset
                    )
                );
            }
            String content = (String) nestedSources.get(chunk.offset).get(SemanticTextField.CHUNKED_TEXT_FIELD);
            if (content == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,

                        "Invalid content detected for field [%s]: missing text for the chunk at offset [%d].",
                        fieldType.name(),
                        chunk.offset
                    )
                );
            }
            snippets[i] = new Text(content);
        }
        return new HighlightField(fieldContext.fieldName, snippets);
    }

    private List<OffsetAndScore> extractOffsetAndScores(
        SearchExecutionContext context,
        LeafReader reader,
        SemanticTextFieldMapper.SemanticTextFieldType fieldType,
        int docId,
        List<Query> leafQueries
    ) throws IOException {
        var bitSet = context.bitsetFilter(fieldType.getChunksField().parentTypeFilter()).getBitSet(reader.getContext());
        int previousParent = docId > 0 ? bitSet.prevSetBit(docId - 1) : -1;

        BooleanQuery.Builder bq = new BooleanQuery.Builder().add(fieldType.getChunksField().nestedTypeFilter(), BooleanClause.Occur.FILTER);
        leafQueries.stream().forEach(q -> bq.add(q, BooleanClause.Occur.SHOULD));
        Weight weight = new IndexSearcher(reader).createWeight(bq.build(), ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.getContext());
        if (previousParent != -1) {
            if (scorer.iterator().advance(previousParent) == DocIdSetIterator.NO_MORE_DOCS) {
                return List.of();
            }
        } else if (scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return List.of();
        }
        List<OffsetAndScore> results = new ArrayList<>();
        int offset = 0;
        while (scorer.docID() < docId) {
            results.add(new OffsetAndScore(offset++, scorer.score()));
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

            @Override
            public void visitLeaf(Query query) {
                if (query instanceof KnnFloatVectorQuery knnQuery) {
                    queries.add(fieldType.createExactKnnQuery(VectorData.fromFloats(knnQuery.getTargetCopy()), null));
                } else if (query instanceof KnnByteVectorQuery knnQuery) {
                    queries.add(fieldType.createExactKnnQuery(VectorData.fromBytes(knnQuery.getTargetCopy()), null));
                }
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
        });
        return queries;
    }
}
