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
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.Bits;

/**
 * A {@link Bits} based iterator.
 */
public class BitsDocIdSetIterator extends MatchDocIdSetIterator {

    private final Bits bits;

    public BitsDocIdSetIterator(Bits bits) {
        super(bits.length());
        this.bits = bits;
    }

    public BitsDocIdSetIterator(int maxDoc, Bits bits) {
        super(maxDoc);
        this.bits = bits;
    }

    @Override
    protected boolean matchDoc(int doc) {
        return bits.get(doc);
    }

    public static class FilteredIterator extends FilteredDocIdSetIterator {

        private final Bits bits;

        FilteredIterator(DocIdSetIterator innerIter, Bits bits) {
            super(innerIter);
            this.bits = bits;
        }

        @Override
        protected boolean match(int doc) {
            return bits.get(doc);
        }
    }

    @Override
    public long cost() {
        return this.bits.length();
    }
}
