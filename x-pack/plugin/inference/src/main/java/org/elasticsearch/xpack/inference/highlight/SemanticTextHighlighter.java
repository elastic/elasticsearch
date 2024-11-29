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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SparseVectorFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceMetaFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.queries.SparseVectorQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SemanticTextHighlighter implements Highlighter {
    public static final String NAME = "semantic";

    private record OffsetAndScore(String sourceField, int startOffset, int endOffset, float score) {}

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType) {
            return true;
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

        int numberOfFragments = fieldContext.field.fieldOptions().numberOfFragments() == 0
            ? 1
            : fieldContext.field.fieldOptions().numberOfFragments();
        var mappingLookup = fieldContext.context.getSearchExecutionContext().getMappingLookup();
        var inferenceMetadata = mappingLookup.inferenceFields().get(fieldContext.fieldName);

        List<OffsetAndScore> chunks = extractOffsetAndScores(
            fieldContext.context.getSearchExecutionContext(),
            fieldContext.hitContext.reader(),
            fieldType,
            fieldContext.hitContext.docId(),
            queries
        );

        Map<String, String> inputs = extractContentFields(fieldContext.hitContext, mappingLookup, inferenceMetadata.getSourceFields());
        chunks.sort(Comparator.comparingDouble(OffsetAndScore::score).reversed());
        int size = Math.min(chunks.size(), numberOfFragments);
        Text[] snippets = new Text[size];
        for (int i = 0; i < size; i++) {
            var chunk = chunks.get(i);
            var content = inputs.get(chunk.sourceField);
            snippets[i] = new Text(content.substring(chunk.startOffset, chunk.endOffset));
        }
        return new HighlightField(fieldContext.fieldName, snippets);
    }

    private Map<String, String> extractContentFields(
        FetchSubPhase.HitContext hitContext,
        MappingLookup mappingLookup,
        String[] sourceFields
    ) throws IOException {
        Map<String, String> inputs = new HashMap<>();
        for (String sourceField : sourceFields) {
            var sourceFieldType = mappingLookup.getFieldType(sourceField);
            if (sourceFieldType == null) {
                continue;
            }
            Object sourceValue = hitContext.source().extractValue(sourceFieldType.name(), null);
            if (sourceValue != null) {
                inputs.put(sourceField, SemanticTextField.nodeStringValues(sourceFieldType.name(), sourceValue));
            }
        }
        return inputs;
    }

    private List<OffsetAndScore> extractOffsetAndScores(
        SearchExecutionContext context,
        LeafReader reader,
        SemanticTextFieldMapper.SemanticTextFieldType fieldType,
        int docID,
        List<Query> leafQueries
    ) throws IOException {
        var bitSet = context.bitsetFilter(fieldType.getChunksField().parentTypeFilter()).getBitSet(reader.getContext());
        int previousParent = docID > 0 ? bitSet.prevSetBit(docID - 1) : -1;

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        leafQueries.stream().forEach(q -> bq.add(q, BooleanClause.Occur.SHOULD));
        Weight weight = new IndexSearcher(reader).createWeight(bq.build(), ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.getContext());
        var terms = reader.terms(OffsetSourceMetaFieldMapper.NAME);
        if (terms == null) {
            // TODO: Empty terms
            return List.of();
        }
        var offsetReader = new OffsetSourceFieldMapper.OffsetsReader(terms, fieldType.getOffsetsField().fullPath());
        if (previousParent != -1) {
            scorer.iterator().advance(previousParent);
        } else if (scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return List.of();
        }
        List<OffsetAndScore> results = new ArrayList<>();
        while (scorer.docID() < docID) {
            if (offsetReader.advanceTo(scorer.docID()) == false) {
                throw new IllegalStateException("Offsets not indexed?");
            }
            results.add(
                new OffsetAndScore(
                    offsetReader.getSourceFieldName(),
                    offsetReader.getStartOffset(),
                    offsetReader.getEndOffset(),
                    scorer.score()
                )
            );
            if (scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
        }
        return results;
    }

    private List<Query> extractDenseVectorQueries(DenseVectorFieldType fieldType, Query querySection) throws IOException {
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

    private List<Query> extractSparseVectorQueries(SparseVectorFieldType fieldType, Query querySection) throws IOException {
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
                if (parent instanceof SparseVectorQuery sparseVectorQuery) {
                    queries.add(sparseVectorQuery.getTermsQuery());
                }
                return this;
            }
        });
        return queries;
    }
}
