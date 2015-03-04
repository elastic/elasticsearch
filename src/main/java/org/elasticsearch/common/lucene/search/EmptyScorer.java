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
package org.elasticsearch.common.lucene.search;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 *
 */
public class EmptyScorer extends Scorer {

    private int docId = -1;

    public EmptyScorer(Weight weight) {
        super(weight);
    }

    @Override
    public float score() throws IOException {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public int freq() throws IOException {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public int docID() {
        return docId;
    }

    @Override
    public int nextDoc() throws IOException {
        assert docId != NO_MORE_DOCS;
        return docId = NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
        return slowAdvance(target);
    }

    @Override
    public long cost() {
        return 0;
    }
}
