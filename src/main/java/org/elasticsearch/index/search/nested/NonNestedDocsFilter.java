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

package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;

import java.io.IOException;

public class NonNestedDocsFilter extends Filter {

    public static final NonNestedDocsFilter INSTANCE = new NonNestedDocsFilter();

    private final PrefixFilter filter = new PrefixFilter(new Term(TypeFieldMapper.NAME, new BytesRef("__")));

    private final int hashCode = filter.hashCode();

    private NonNestedDocsFilter() {

    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        DocIdSet docSet = filter.getDocIdSet(context, acceptDocs);
        if (DocIdSets.isEmpty(docSet)) {
            // will almost never happen, and we need an OpenBitSet for the parent filter in
            // BlockJoinQuery, we cache it anyhow...
            docSet = new FixedBitSet(context.reader().maxDoc());
        }
        ((FixedBitSet) docSet).flip(0, context.reader().maxDoc());
        return docSet;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == INSTANCE;
    }
}