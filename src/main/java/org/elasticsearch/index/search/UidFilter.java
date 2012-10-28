/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.search;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class UidFilter extends Filter {

    final Term[] uids;
    public UidFilter(Collection<String> types, List<String> ids) {
        this.uids = new Term[types.size() * ids.size()];
        int i = 0;
        for (String type : types) {
            for (String id : ids) {
                uids[i++] = new Term(UidFieldMapper.NAME, Uid.createUid(type, id));
            }
        }
        if (this.uids.length > 1) {
            Arrays.sort(this.uids);
        }
    }

    public Term[] getTerms() {
        return this.uids;
    }

    // TODO Optimizations
    // - If we have a single id, we can create a SingleIdDocIdSet to save on mem
    // - We can use sorted int array DocIdSet to reserve memory compared to OpenBitSet in some cases
    @Override
    // LUCENE 4 UPGRADE: this filter does respect acceptDocs maybe we need to change this
    public DocIdSet getDocIdSet(AtomicReaderContext ctx, Bits acceptedDocs) throws IOException {
        FixedBitSet set = null;
        final AtomicReader reader = ctx.reader();
        final TermsEnum termsEnum = reader.terms(UidFieldMapper.NAME).iterator(null);
        DocsEnum docsEnum = null;
        for (Term uid : uids) {
            if (termsEnum.seekExact(uid.bytes(), false)) {
                docsEnum = termsEnum.docs(acceptedDocs, docsEnum, 0);
                int doc;
                while ((doc = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
                    // no need for batching, its on the UID, there will be only
                    // one doc
                    if (set == null) {
                        set = new FixedBitSet(reader.maxDoc());
                    }
                    set.set(doc);
                }
            }

        }
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UidFilter uidFilter = (UidFilter) o;
        return Arrays.equals(uids, uidFilter.uids);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(uids);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Term term : uids) {
            if (builder.length() > 0) {
                builder.append(' ');
            }
            builder.append(term);
        }
        return builder.toString();
    }
}