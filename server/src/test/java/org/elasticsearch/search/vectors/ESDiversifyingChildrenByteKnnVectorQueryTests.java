/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ByteKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;

public class ESDiversifyingChildrenByteKnnVectorQueryTests extends AbstractESDiversifyingChildrenKnnVectorQueryTestCase {

    @Override
    Query getParentJoinKnnQuery(String fieldName, float[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet) {
        return new ESDiversifyingChildrenKnnVectorQuery(
            new DiversifyingChildrenByteKnnVectorQuery(fieldName, fromFloat(queryVector), childFilter, k, parentBitSet),
            new BruteForceKnnQuery(
                new ByteVectorSimilarityFunction(
                    VectorSimilarityFunction.EUCLIDEAN,
                    new ByteKnnVectorFieldSource(fieldName),
                    new ConstKnnByteVectorValueSource(fromFloat(queryVector))
                )
            ),
            parentBitSet
        );
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnByteVectorField(name, fromFloat(vector));
    }

}
