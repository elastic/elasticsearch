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

package org.elasticsearch.painless;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Collections;

public class ScoreTests extends ScriptTestCase {

    /** Most of a dummy scorer impl that requires overriding just score(). */
    abstract class MockScorer extends Scorer {
        MockScorer() {
            super(null);
        }
        @Override
        public int docID() {
            return 0;
        }
        @Override
        public int freq() throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }
    }

    public void testScoreWorks() {
        assertEquals(2.5, exec("_score", Collections.emptyMap(), Collections.emptyMap(),
            new MockScorer() {
                @Override
                public float score() throws IOException {
                    return 2.5f;
                }
            },
            true));
    }

    public void testScoreNotUsed() {
        assertEquals(3.5, exec("3.5", Collections.emptyMap(), Collections.emptyMap(),
            new MockScorer() {
                @Override
                public float score() throws IOException {
                    throw new AssertionError("score() should not be called");
                }
            },
            true));
    }

    public void testScoreCached() {
        assertEquals(9.0, exec("_score + _score", Collections.emptyMap(), Collections.emptyMap(),
            new MockScorer() {
                private boolean used = false;
                @Override
                public float score() throws IOException {
                    if (used == false) {
                        return 4.5f;
                    }
                    throw new AssertionError("score() should not be called twice");
                }
            },
            true));
    }
}
