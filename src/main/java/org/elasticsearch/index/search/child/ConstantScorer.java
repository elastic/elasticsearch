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
package org.elasticsearch.index.search.child;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * A scorer that wraps a {@link DocIdSetIterator} and emits a constant score.
 */
// Borrowed from ConstantScoreQuery
public class ConstantScorer extends Scorer {

    public static ConstantScorer create(DocIdSetIterator iterator, Weight weight, float constantScore) throws IOException {
        return new ConstantScorer(iterator, weight, constantScore);
    }

    private final DocIdSetIterator docIdSetIterator;
    private final float constantScore;

    private ConstantScorer(DocIdSetIterator docIdSetIterator, Weight w, float constantScore) {
        super(w);
        this.constantScore = constantScore;
        this.docIdSetIterator = docIdSetIterator;
    }

    @Override
    public int nextDoc() throws IOException {
        return docIdSetIterator.nextDoc();
    }

    @Override
    public int docID() {
        return docIdSetIterator.docID();
    }

    @Override
    public float score() throws IOException {
        assert docIdSetIterator.docID() != NO_MORE_DOCS;
        return constantScore;
    }

    @Override
    public int freq() throws IOException {
        return 1;
    }

    @Override
    public int advance(int target) throws IOException {
        return docIdSetIterator.advance(target);
    }

    @Override
    public long cost() {
        return docIdSetIterator.cost();
    }
}