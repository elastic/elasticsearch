/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;

import java.util.Objects;

final class MergedDocValuesRangeQuery {

    private MergedDocValuesRangeQuery() {}

    /**
     * Attempt to merge together two overlapping NumericDocValuesRangeQuery instances.  Returns
     * a merged query if successful, otherwise {@code null}
     */
    public static Query merge(Query query, Query extraQuery) {

        if (query instanceof NumericDocValuesRangeQuery query1 && extraQuery instanceof NumericDocValuesRangeQuery query2) {

            if (Objects.equals(query1.getField(), query2.getField()) == false) {
                return null;
            }

            long q1LowerValue = query1.lowerValue();
            long q1UpperValue = query1.upperValue();
            long q2LowerValue = query2.lowerValue();
            long q2UpperValue = query2.upperValue();

            if (q1UpperValue < q2LowerValue || q2UpperValue < q1LowerValue) {
                return new MatchNoDocsQuery("Non-overlapping range queries");
            }

            return NumericDocValuesField.newSlowRangeQuery(
                query1.getField(),
                Math.max(q1LowerValue, q2LowerValue),
                Math.min(q1UpperValue, q2UpperValue)
            );
        }

        return null;
    }
}
