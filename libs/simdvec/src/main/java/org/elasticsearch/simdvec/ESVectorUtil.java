/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;

import static org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport.B_QUERY;

public class ESVectorUtil {

    private static final ESVectorUtilSupport IMPL = ESVectorizationProvider.getInstance().getVectorUtilSupport();

    public static long ipByteBinByte(byte[] q, byte[] d) {
        if (q.length != d.length * B_QUERY) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + q.length + "!= " + B_QUERY + " x " + d.length);
        }
        return IMPL.ipByteBinByte(q, d);
    }
}
