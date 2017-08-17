/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;

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
        if (fv.advanceExact(docId))
            return Explanation.match((float)fv.doubleValue(), "ReplaceableConstDoubleValues");
        else
            return Explanation.noMatch("ReplaceableConstDoubleValues");
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
}
