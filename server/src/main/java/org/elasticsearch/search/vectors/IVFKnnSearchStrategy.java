/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.search.knn.KnnSearchStrategy;

import java.util.Objects;

public class IVFKnnSearchStrategy extends KnnSearchStrategy {
    private final int nProbe;

    IVFKnnSearchStrategy(int nProbe) {
        this.nProbe = nProbe;
    }

    public int getNProbe() {
        return nProbe;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IVFKnnSearchStrategy that = (IVFKnnSearchStrategy) o;
        return nProbe == that.nProbe;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nProbe);
    }

    @Override
    public void nextVectorsBlock() {
        // do nothing
    }
}
