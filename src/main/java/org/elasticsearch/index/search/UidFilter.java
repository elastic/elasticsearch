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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.*;

public class UidFilter extends Filter {

    private final Set<Term> uids;

    private final BloomCache bloomCache;

    public UidFilter(Collection<String> types, List<String> ids, BloomCache bloomCache) {
        this.bloomCache = bloomCache;
        this.uids = new TreeSet<Term>();
        for (String type : types) {
            for (String id : ids) {
                uids.add(UidFieldMapper.TERM_FACTORY.createTerm(Uid.createUid(type, id)));
            }
        }
    }

    // TODO Optimizations
    // - If we have a single id, we can create a SingleIdDocIdSet to save on mem
    // - We can use sorted int array DocIdSet to reserve memory compared to OpenBitSet in some cases
    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        BloomFilter filter = bloomCache.filter(reader, UidFieldMapper.NAME, true);
        FixedBitSet set = null;
        TermDocs td = null;
        try {
            for (Term uid : uids) {
                UnicodeUtil.UTF8Result utf8 = Unicode.fromStringAsUtf8(uid.text());
                if (!filter.isPresent(utf8.result, 0, utf8.length)) {
                    continue;
                }
                if (td == null) {
                    td = reader.termDocs();
                }
                td.seek(uid);
                while (td.next()) {
                    if (set == null) {
                        set = new FixedBitSet(reader.maxDoc());
                    }
                    set.set(td.doc());
                }
            }
        } finally {
            if (td != null) {
                td.close();
            }
        }
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UidFilter uidFilter = (UidFilter) o;
        return !uids.equals(uidFilter.uids);
    }

    @Override
    public String toString() {
        return "UidFilter(" + uids + ")";
    }

    @Override
    public int hashCode() {
        return uids.hashCode();
    }
}