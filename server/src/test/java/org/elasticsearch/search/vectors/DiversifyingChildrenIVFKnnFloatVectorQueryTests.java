/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;

public class DiversifyingChildrenIVFKnnFloatVectorQueryTests extends AbstractDiversifyingChildrenIVFKnnVectorQueryTestCase {

    @Override
    Query getDiversifyingChildrenKnnQuery(String fieldName, float[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet) {
        return new DiversifyingChildrenIVFKnnFloatVectorQuery(fieldName, queryVector, k, k, childFilter, parentBitSet, -1);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }
}
