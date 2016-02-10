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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public abstract class ScoreFunction {

    private final CombineFunction scoreCombiner;

    protected ScoreFunction(CombineFunction scoreCombiner) {
        this.scoreCombiner = scoreCombiner;
    }

    public CombineFunction getDefaultScoreCombiner() {
        return scoreCombiner;
    }

    public abstract LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException;

    /**
     * Indicates if document scores are needed by this function.
     * 
     * @return {@code true} if scores are needed.
     */
    public abstract boolean needsScores();

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ScoreFunction other = (ScoreFunction) obj;
        return Objects.equals(scoreCombiner, other.scoreCombiner) &&
                doEquals(other);
    }

    /**
     * Indicates whether some other {@link ScoreFunction} object of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(ScoreFunction other);

    @Override
    public final int hashCode() {
        /*
         * Override hashCode here and forward to an abstract method to force extensions of this class to override hashCode in the same
         * way that we force them to override equals. This also prevents false positives in CheckStyle's EqualsHashCode check.
         */
        return Objects.hash(scoreCombiner, doHashCode());
    }

    protected abstract int doHashCode();
}
