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
import org.elasticsearch.index.IndexVersions;
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
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryWrapper;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceField;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceMetaFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link Highlighter} designed for the {@link SemanticTextFieldMapper}.
 * It extracts semantic queries and compares them against each chunk in the document.
 * The top-scoring chunks are returned as snippets, sorted by their scores.
 */
public class SemanticTextHighlighter implements Highlighter {
    public static final String NAME = "semantic";

    private record OffsetAndScore(OffsetSourceFieldMapper.OffsetSource offset, float score) {}

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            // TODO: Handle semantic text field prior to the inference metadata fields version.
            return semanticTextFieldType.getIndexVersionCreated().onOrAfter(IndexVersions.INFERENCE_METADATA_FIELDS);
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
            ? 1 // we return the best fragment by default
            : fieldContext.field.fieldOptions().numberOfFragments();

        var mappingLookup = fieldContext.context.getSearchExecutionContext().getMappingLookup();

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
            chunks.subList(0, size).sort(Comparator.comparingDouble(c -> c.offset.start()));
        }
        Map<String, String> inputs = new HashMap<>();
        Text[] snippets = new Text[size];
        for (int i = 0; i < size; i++) {
            var chunk = chunks.get(i);
            var content = inputs.computeIfAbsent(chunk.offset.field(), k -> extractFieldContent(fieldContext.hitContext, mappingLookup, k));
            if (content == null) {
                throw new IllegalStateException("Missing content for field [" + chunk.offset.field() + "]");
            }
            if (chunk.offset().start() == -1
                || chunk.offset.end() == -1
                || chunk.offset().start() > content.length()
                || chunk.offset().end() > content.length()) {
                throw new IllegalStateException(
                    "Offset ["
                        + chunk.offset()
                        + "] computed for the field ["
                        + fieldType.name()
                        + "] do not match the actual content of the field."
                );
            }
            snippets[i] = new Text(content.substring(chunk.offset().start(), chunk.offset().end()));
        }
        return new HighlightField(fieldContext.fieldName, snippets);
    }

    private String extractFieldContent(FetchSubPhase.HitContext hitContext, MappingLookup mappingLookup, String sourceField) {
        var sourceFieldType = mappingLookup.getFieldType(sourceField);
        if (sourceFieldType == null) {
            return null;
        }
        // TODO: Consider using a value fetcher here, as it will work if the field is stored, but ensure it excludes values derived from
        // copy_to fields.
        Object sourceValue = hitContext.source().extractValue(sourceFieldType.name(), null);
        return sourceValue != null ? SemanticTextUtils.nodeStringValues(sourceFieldType.name(), sourceValue) : null;
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

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        leafQueries.stream().forEach(q -> bq.add(q, BooleanClause.Occur.SHOULD));
        Weight weight = new IndexSearcher(reader).createWeight(bq.build(), ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.getContext());
        var terms = reader.terms(OffsetSourceMetaFieldMapper.NAME);
        if (terms == null) {
            // The field is empty
            return List.of();
        }
        var offsetReader = OffsetSourceField.loader(terms, fieldType.getOffsetsField().fullPath());
        if (previousParent != -1) {
            if (scorer.iterator().advance(previousParent) == DocIdSetIterator.NO_MORE_DOCS) {
                return List.of();
            }
        } else if (scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return List.of();
        }
        List<OffsetAndScore> results = new ArrayList<>();
        while (scorer.docID() < docId) {
            var offset = offsetReader.advanceTo(scorer.docID());
            if (offset == null) {
                throw new IllegalStateException(
                    "Cannot highlight field [" + fieldType.name() + "], missing embeddings for doc [" + docId + "]"
                );
            }
            results.add(new OffsetAndScore(offset, scorer.score()));
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
