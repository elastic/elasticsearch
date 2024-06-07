/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.utils;

public class FloatConversionUtils {

    public static float[] floatArrayOf(double[] doublesArray) {
        var floatArray = new float[doublesArray.length];
        for (int i = 0; i < doublesArray.length; i++) {
            floatArray[i] = (float) doublesArray[i];
        }
        return floatArray;
    }

}
