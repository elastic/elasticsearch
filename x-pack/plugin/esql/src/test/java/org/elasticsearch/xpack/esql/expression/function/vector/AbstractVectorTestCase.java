/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractVectorTestCase extends AbstractScalarFunctionTestCase {

    static final double DELTA = 0.000001f;

    protected static float[] listToFloatArray(List<Float> floatList) {
        float[] floatArray = new float[floatList.size()];
        for (int i = 0; i < floatList.size(); i++) {
            floatArray[i] = floatList.get(i);
        }
        return floatArray;
    }

    /**
     * @return A random dense vector for testing
     * @param dimensions
     */
    protected static List<Float> randomDenseVector(int dimensions) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimensions; i++) {
            vector.add(randomFloat());
        }
        return vector;
    }

}
