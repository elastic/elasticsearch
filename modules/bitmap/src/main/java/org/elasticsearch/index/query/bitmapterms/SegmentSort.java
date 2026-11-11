/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * The index-sort precondition both bitmap queries share. Under an ascending sort on the queried field
 * doc order follows value order, which is what lets {@link BitmapTermsQuery} and {@link BitmapBKDQuery}
 * stream their matches instead of collecting them into a
 * {@link org.apache.lucene.util.DocIdSetBuilder} to put them in order.
 */
final class SegmentSort {

    private SegmentSort() {}

    /** Whether the segment's primary sort is an ascending sort on {@code field}. */
    static boolean ascendingBy(LeafReader reader, String field) {
        Sort sort = reader.getMetaData().sort();
        if (sort == null || sort.getSort().length == 0) {
            return false;
        }
        SortField primary = sort.getSort()[0];
        return field.equals(primary.getField()) && primary.getReverse() == false;
    }
}
