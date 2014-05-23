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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;

import java.io.IOException;

public class LimitFilter extends NoCacheFilter {

    private final int limit;
    private final boolean short_circuit;
    private int counter;

    public LimitFilter(int limit, boolean short_circuit) {
        this.limit = limit;
        this.short_circuit = short_circuit;
    }

    public int getLimit() {
        return limit;
    }

    public boolean getShortCircuit() {
        return short_circuit;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        if (counter > limit) {
            return null;
        }
        return new LimitDocIdSet(context.reader().maxDoc(), acceptDocs);
    }

    public class LimitDocIdSet extends MatchDocIdSet {

        public LimitDocIdSet(int maxDoc, @Nullable Bits acceptDocs) {
            super(maxDoc, acceptDocs);
        }

        @Override
        protected boolean matchDoc(int doc) {
            if (++counter > limit) {
                if (short_circuit)
                    throw new RuntimeException("Limited results to: " + counter);
                return false;
            }
            return true;
        }
    }
}
