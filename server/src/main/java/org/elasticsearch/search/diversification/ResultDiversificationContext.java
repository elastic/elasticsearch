/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.vectors.VectorData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class ResultDiversificationContext {
    private static final VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

    private final String field;
    private final int size;
    private final Supplier<VectorData> queryVectorSupplier;
    private final Map<Integer, List<VectorData>> fieldVectors = new HashMap<>();

    private boolean retrievedQueryVector = false;
    private VectorData realizedQueryVector = null;

    protected ResultDiversificationContext(String field, int size, @Nullable Supplier<VectorData> queryVector) {
        this.field = field;
        this.size = size;
        this.queryVectorSupplier = queryVector;
    }

    public String getField() {
        return field;
    }

    public int getSize() {
        return size;
    }

    public int setFieldVectors(
        DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits,
        FieldVectorSupplier fieldVectorSupplier,
        int numVectorsLimit
    ) {
        VectorData queryVector = this.getQueryVector();

        var documentFieldVectors = fieldVectorSupplier.getFieldVectors(searchHits);
        for (var documentFieldVector : documentFieldVectors.entrySet()) {
            List<VectorData> vectorData = getHitVectorData(documentFieldVector, queryVector, numVectorsLimit);
            this.fieldVectors.put(documentFieldVector.getKey(), vectorData);
        }
        return this.fieldVectors.size();
    }

    private List<VectorData> getHitVectorData(
        Map.Entry<Integer, List<VectorData>> documentFieldVector,
        VectorData queryVector,
        int numVectorsLimit
    ) {

        var fieldVectors = documentFieldVector.getValue();

        if (queryVector != null) {
            List<Tuple<Integer, Float>> scores = new ArrayList<>();
            for (int v = 0; v < fieldVectors.size(); v++) {
                scores.add(new Tuple<>(v, getVectorSimilarity(queryVector, fieldVectors.get(v))));
            }

            // sort chunks by score descending
            scores.sort(new Comparator<Tuple<Integer, Float>>() {
                @Override
                public int compare(Tuple<Integer, Float> o1, Tuple<Integer, Float> o2) {
                    if (o1.v2() > o2.v2()) {
                        return -1;
                    } else if (o1.v2() < o2.v2()) {
                        return 1;
                    }
                    return 0;
                }
            });

            List<VectorData> topSimilarVectors = new ArrayList<>();
            for (int i = 0; i < scores.size() && i < numVectorsLimit; i++) {
                topSimilarVectors.add(fieldVectors.get(scores.get(i).v1()));
            }
            return topSimilarVectors;
        }

        if (fieldVectors.size() <= numVectorsLimit) {
            return fieldVectors;
        }

        return fieldVectors.subList(0, numVectorsLimit);
    }

    public VectorData getQueryVector() {
        if (retrievedQueryVector) {
            return realizedQueryVector;
        }
        realizedQueryVector = queryVectorSupplier == null ? null : queryVectorSupplier.get();
        retrievedQueryVector = true;
        return realizedQueryVector;
    }

    public List<VectorData> getFieldVectorData(int rank) {
        return fieldVectors.getOrDefault(rank, null);
    }

    protected float getVectorSimilarity(VectorData first, VectorData second) {
        return first.isFloat() ? getFloatVectorComparisonScore(first, second) : getByteVectorComparisonScore(first, second);
    }

    protected float getFloatVectorComparisonScore(VectorData thisDocVector, VectorData comparisonVector) {
        return similarityFunction.compare(thisDocVector.floatVector(), comparisonVector.floatVector());
    }

    protected float getByteVectorComparisonScore(VectorData thisDocVector, VectorData comparisonVector) {
        return similarityFunction.compare(thisDocVector.byteVector(), comparisonVector.byteVector());
    }
}
