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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;

/**
 * A {@link SortedDocsProducer} that can sort documents based on terms indexed in the provided field.
 */
class TermsSortedDocsProducer extends SortedDocsProducer {
    TermsSortedDocsProducer(String field) {
        super(field);
    }

    @Override
    DocIdSet processLeaf(Query query, CompositeValuesCollectorQueue queue,
                         LeafReaderContext context, boolean fillDocIdSet) throws IOException {
        final Terms terms = context.reader().terms(field);
        if (terms == null) {
            // no value for the field
            return DocIdSet.EMPTY;
        }
        BytesRef lowerValue = (BytesRef) queue.getLowerValueLeadSource();
        BytesRef upperValue = (BytesRef) queue.getUpperValueLeadSource();
        final TermsEnum te = terms.iterator();
        if (lowerValue != null) {
            if (te.seekCeil(lowerValue) == TermsEnum.SeekStatus.END) {
                return DocIdSet.EMPTY ;
            }
        } else {
            if (te.next() == null) {
                return DocIdSet.EMPTY;
            }
        }
        DocIdSetBuilder builder = fillDocIdSet ? new DocIdSetBuilder(context.reader().maxDoc(), terms) : null;
        PostingsEnum reuse = null;
        boolean first = true;
        final BytesRef upper = upperValue == null ? null : BytesRef.deepCopyOf(upperValue);
        do {
            if (upper != null && upper.compareTo(te.term()) < 0) {
                break;
            }
            reuse = te.postings(reuse, PostingsEnum.NONE);
            if (processBucket(queue, context, reuse, te.term(), builder) && !first) {
                // this bucket does not have any competitive composite buckets,
                // we can early terminate the collection because the remaining buckets are guaranteed
                // to be greater than this bucket.
                break;
            }
            first = false;
        } while (te.next() != null);
        return fillDocIdSet ? builder.build() : DocIdSet.EMPTY;
    }
}
