/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;

/**
 * Advantages over using this filter over Lucene's TermsFilter in the parent child context:
 * 1) Don't need to copy all values over to a list from the id cache and then
 *    copy all the ids values over to one continuous byte array. Should save a lot of of object creations and gcs..
 * 2) We filter docs by one field only.
 * 3) We can directly reference to values that originate from the id cache.
 */
final class ParentIdsFilter extends Filter {

    private final BytesRef parentTypeBr;
    private final Object[] keys;
    private final boolean[] allocated;

    private final Filter nonNestedDocsFilter;

    public ParentIdsFilter(String parentType, Object[] keys, boolean[] allocated, Filter nonNestedDocsFilter) {
        this.nonNestedDocsFilter = nonNestedDocsFilter;
        this.parentTypeBr = new BytesRef(parentType);
        this.keys = keys;
        this.allocated = allocated;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(UidFieldMapper.NAME);
        if (terms == null) {
            return null;
        }

        TermsEnum termsEnum = terms.iterator(null);
        BytesRef uidSpare = new BytesRef();
        BytesRef idSpare = new BytesRef();

        if (acceptDocs == null) {
            acceptDocs = context.reader().getLiveDocs();
        }

        FixedBitSet nonNestedDocs = null;
        if (nonNestedDocsFilter != null) {
            nonNestedDocs = (FixedBitSet) nonNestedDocsFilter.getDocIdSet(context, acceptDocs);
        }

        DocsEnum docsEnum = null;
        FixedBitSet result = null;
        for (int i = 0; i < allocated.length; i++) {
            if (!allocated[i]) {
                continue;
            }

            idSpare.bytes = ((HashedBytesArray) keys[i]).toBytes();
            idSpare.length = idSpare.bytes.length;
            Uid.createUidAsBytes(parentTypeBr, idSpare, uidSpare);
            if (termsEnum.seekExact(uidSpare)) {
                int docId;
                docsEnum = termsEnum.docs(acceptDocs, docsEnum, DocsEnum.FLAG_NONE);
                if (result == null) {
                    docId = docsEnum.nextDoc();
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        result = new FixedBitSet(context.reader().maxDoc());
                    } else {
                        continue;
                    }
                } else {
                    docId = docsEnum.nextDoc();
                    if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                        continue;
                    }
                }
                if (nonNestedDocs != null && !nonNestedDocs.get(docId)) {
                    docId = nonNestedDocs.nextSetBit(docId);
                }
                result.set(docId);
                assert docsEnum.advance(docId + 1) == DocIdSetIterator.NO_MORE_DOCS : "DocId " + docId + " should have been the last one but docId " + docsEnum.docID() + " exists.";
            }
        }
        return result;
    }
}