/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.chunks;

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
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceField;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for extracting and scoring {@link SemanticTextField} chunks.
 * Used for both chunk scoring and highlighting use cases.
 */
public class SemanticTextChunkUtils {

    private SemanticTextChunkUtils() {}

    public record OffsetAndScore(int index, OffsetSourceFieldMapper.OffsetSource offset, float score) {}

    public static String extractContent(OffsetAndScore offsetAndScore, DocumentField docFieldContent) {
        String content = null;
        if (docFieldContent != null && docFieldContent.getValues().size() > 0) {
            String fullContent = docFieldContent.getValue().toString();
            content = fullContent.substring(offsetAndScore.offset().start(), offsetAndScore.offset().end());
        }
        return content;
    }

    public static String getContentFromLegacyNestedSources(String fieldName, OffsetAndScore cand, List<Map<?, ?>> nestedSources) {
        if (nestedSources.size() <= cand.index()) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Invalid content detected for field [%s]: the chunks size is [%d], "
                        + "but a reference to offset [%d] was found in the result.",
                    fieldName,
                    nestedSources.size(),
                    cand.index()
                )
            );
        }
        return (String) nestedSources.get(cand.index()).get(SemanticTextField.CHUNKED_TEXT_FIELD);
    }

    public static List<Query> queries(FieldMapper embeddingsField, TaskType taskType, Query query) throws IOException {
        final List<Query> queries = switch (taskType) {
            case SPARSE_EMBEDDING -> extractSparseVectorQueries(
                (SparseVectorFieldMapper.SparseVectorFieldType) embeddingsField.fieldType(),
                query
            );
            case TEXT_EMBEDDING -> extractDenseVectorQueries(
                (DenseVectorFieldMapper.DenseVectorFieldType) embeddingsField.fieldType(),
                query
            );
            default -> throw new IllegalStateException("Wrong task type for a semantic text field, got [" + taskType.name() + "]");
        };
        return queries;
    }

    public static List<OffsetAndScore> extractOffsetAndScores(
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
        if (scorer == null) {
            return java.util.List.of();
        }
        if (previousParent != -1) {
            if (scorer.iterator().advance(previousParent) == DocIdSetIterator.NO_MORE_DOCS) {
                return java.util.List.of();
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

    private static List<Query> extractDenseVectorQueries(DenseVectorFieldType fieldType, Query querySection) {
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
                    queries.add(new MatchAllDocsQuery());
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

    public static List<Query> extractSparseVectorQueries(SparseVectorFieldMapper.SparseVectorFieldType fieldType, Query querySection) {
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
                    queries.add(new MatchAllDocsQuery());
                }
            }
        });
        return queries;
    }

}
