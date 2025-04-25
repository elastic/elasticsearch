/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
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
    DocIdSet processLeaf(CompositeValuesCollectorQueue queue, LeafReaderContext context, boolean fillDocIdSet) throws IOException {
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
                return DocIdSet.EMPTY;
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
            if (processBucket(queue, context, reuse, te.term(), builder) && first == false) {
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
