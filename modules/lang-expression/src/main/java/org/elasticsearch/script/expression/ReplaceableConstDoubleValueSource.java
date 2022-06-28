/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

/**
 * A {@link DoubleValuesSource} which has a stub {@link DoubleValues} that holds a dynamically replaceable constant double.
 */
final class ReplaceableConstDoubleValueSource extends DoubleValuesSource {
    final ReplaceableConstDoubleValues fv;

    ReplaceableConstDoubleValueSource() {
        fv = new ReplaceableConstDoubleValues();
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        return fv;
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
        if (fv.advanceExact(docId)) return Explanation.match((float) fv.doubleValue(), "ReplaceableConstDoubleValues");
        else return Explanation.noMatch("ReplaceableConstDoubleValues");
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public void setValue(double v) {
        fv.setValue(v);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
        return this;
    }
}
