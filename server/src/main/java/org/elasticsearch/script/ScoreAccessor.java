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

package org.elasticsearch.script;

import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.lookup.DocLookup;

import java.io.IOException;

/**
 * A float encapsulation that dynamically accesses the score of a document.
 *
 * The provided {@link DocLookup} is used to retrieve the score
 * for the current document.
 */
public final class ScoreAccessor extends Number implements Comparable<Number> {

    Scorer scorer;

    public ScoreAccessor(Scorer scorer) {
        this.scorer = scorer;
    }

    float score() {
        try {
            return scorer.score();
        } catch (IOException e) {
            throw new RuntimeException("Could not get score", e);
        }
    }

    @Override
    public int intValue() {
        return (int)score();
    }

    @Override
    public long longValue() {
        return (long)score();
    }

    @Override
    public float floatValue() {
        return score();
    }

    @Override
    public double doubleValue() {
        return score();
    }

    @Override
    public int compareTo(Number o) {
        return Float.compare(this.score(), o.floatValue());
    }
}
