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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 */
public abstract class MatchDocIdSetIterator extends DocIdSetIterator {
    private final int maxDoc;
    private int doc = -1;

    public MatchDocIdSetIterator(int maxDoc) {
        this.maxDoc = maxDoc;
    }

    protected abstract boolean matchDoc(int doc);

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        do {
            doc++;
            if (doc >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
        } while (!matchDoc(doc));
        return doc;
    }

    @Override
    public int advance(int target) throws IOException {
        if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
        }
        doc = target;
        while (!matchDoc(doc)) {
            doc++;
            if (doc >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
        }
        return doc;
    }
}
