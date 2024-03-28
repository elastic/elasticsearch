/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.lang.invoke.MethodHandle;

public final class VectorSimilarityFunctions {

    private final VectorLibrary vectorLibrary;

    VectorSimilarityFunctions(VectorLibrary vectorLibrary) {
        this.vectorLibrary = vectorLibrary;
    }

    public MethodHandle dotProductHandle() {
        return vectorLibrary.dotProductHandle();
    }

    public MethodHandle squareDistanceHandle() {
        return vectorLibrary.squareDistanceHandle();
    }
}
