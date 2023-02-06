/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rerank;

public class RRFReranker implements Reranker {

    protected final int size;
    protected final int windowSize;
    protected final int kConstant;

    public RRFReranker(int windowSize, int size, int kConstant) {
        this.windowSize = windowSize;
        this.size = size;
        this.kConstant = kConstant;
    }

    @Override
    public int windowSize() {
        return windowSize;
    }

    @Override
    public int size() {
        return size;
    }

    public int kConstant() {
        return kConstant;
    }
}
