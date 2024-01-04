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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;

public class ESDiversifyingChildrenByteKnnVectorQueryTests extends AbstractESDiversifyingChildrenKnnVectorQueryTestCase {

    @Override
    Query getParentJoinKnnQuery(
        String fieldName,
        float[] queryVector,
        Query childFilter,
        int k,
        BitSetProducer parentBitSet,
        int numChildrenPerParent
    ) {
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            fieldName,
            fromFloat(queryVector),
            childFilter,
            k,
            parentBitSet,
            numChildrenPerParent
        );
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnByteVectorField(name, fromFloat(vector));
    }

}
