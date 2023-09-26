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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link DoubleValuesSource} which has a stub {@link DoubleValues} that holds a dynamically replaceable constant double.
 */
final class ReplaceableConstDoubleValueSource extends DoubleValuesSource {

    private final Map<LeafReaderContext, ReplaceableConstDoubleValues> specialValues = new ConcurrentHashMap<>();

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        ReplaceableConstDoubleValues replaceableConstDoubleValues = new ReplaceableConstDoubleValues();
        specialValues.put(ctx, replaceableConstDoubleValues);
        return replaceableConstDoubleValues;
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) {
        throw new UnsupportedOperationException("explain is not supported for _value and should never be called");
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public void setValue(LeafReaderContext ctx, double v) {
        ReplaceableConstDoubleValues fv = specialValues.get(ctx);
        assert fv != null : "getValues must be called before setValue for any given leaf reader context";
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
