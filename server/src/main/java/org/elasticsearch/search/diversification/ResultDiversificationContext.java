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
import java.util.Set;
import java.util.function.Supplier;

public abstract class ResultDiversificationContext {
    private static final VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

    private final String field;
    private final int size;
    private final Supplier<VectorData> queryVector;
    private final Map<Integer, List<Tuple<Integer, VectorData>>> fieldVectors = new HashMap<>();

    private boolean retrievedQueryVector = false;
    private VectorData realizedQueryVector = null;

    protected ResultDiversificationContext(String field, int size, @Nullable Supplier<VectorData> queryVector) {
        this.field = field;
        this.size = size;
        this.queryVector = queryVector;
    }

    public String getField() {
        return field;
    }

    public int getSize() {
        return size;
    }

    public int setFieldVectors(FieldVectorSupplier fieldVectorSupplier) {
        var vectors = fieldVectorSupplier.getFieldVectors();
        VectorData queryVector = this.getQueryVector();
        Boolean useFloat = null;
        for (var vector : vectors.entrySet()) {
            Map<Integer, VectorData> vectorTuples = new HashMap<>();
            var fieldVector = vector.getValue();
            for (int i = 0; i < fieldVector.size(); i++) {
                if (useFloat == null) {
                    useFloat = fieldVector.get(i).isFloat();
                }
                vectorTuples.put(i, fieldVector.get(i));
            }

            List<Tuple<Integer, VectorData>> sortedTuples = new ArrayList<>();
            if (queryVector != null) {
                // uuuugly sort - redo this later
                List<Tuple<Integer, Float>> scores = new ArrayList<>();
                for (var vec : vectorTuples.entrySet()) {
                    scores.add(new Tuple<>(vec.getKey(), getVectorSimilarity(useFloat, queryVector, vec.getValue())));
                }
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

                for (var sortedTuple : scores) {
                    sortedTuples.add(new Tuple<>(sortedTuple.v1(), vectorTuples.get(sortedTuple.v1())));
                }
            } else {
                for (var vec : vectorTuples.entrySet()) {
                    sortedTuples.add(new Tuple<>(vec.getKey(), vec.getValue()));
                }
            }
            this.fieldVectors.put(vector.getKey(), sortedTuples);
        }
        return this.fieldVectors.size();
    }

    public VectorData getQueryVector() {
        if (retrievedQueryVector) {
            return realizedQueryVector;
        }
        realizedQueryVector = queryVector == null ? null : queryVector.get();
        retrievedQueryVector = true;
        return realizedQueryVector;
    }

    public List<Tuple<Integer, VectorData>> getFieldVectorData(int rank) {
        return fieldVectors.getOrDefault(rank, null);
    }

    public Set<Map.Entry<Integer, List<Tuple<Integer, VectorData>>>> getFieldVectorsEntrySet() {
        return fieldVectors.entrySet();
    }

    protected float getVectorSimilarity(boolean useFloat, VectorData first, VectorData second) {
        return useFloat ? getFloatVectorComparisonScore(first, second) : getByteVectorComparisonScore(first, second);
    }

    protected float getFloatVectorComparisonScore(VectorData thisDocVector, VectorData comparisonVector) {
        return similarityFunction.compare(thisDocVector.floatVector(), comparisonVector.floatVector());
    }

    protected float getByteVectorComparisonScore(VectorData thisDocVector, VectorData comparisonVector) {
        return similarityFunction.compare(thisDocVector.byteVector(), comparisonVector.byteVector());
    }
}
