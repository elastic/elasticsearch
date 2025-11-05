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
import org.apache.lucene.search.Query;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Field;
import java.util.Objects;

class MergedDocValuesRangeQuery {

    @SuppressForbidden(reason = "Uses reflection to access package-private lucene class")
    public static Query merge(Query query, Query extraQuery) {
        Class<? extends Query> queryClass = query.getClass();
        Class<? extends Query> extraQueryClass = extraQuery.getClass();

        if (queryClass.equals(extraQueryClass) == false
            || queryClass.getCanonicalName().equals("org.apache.lucene.document.SortedNumericDocValuesRangeQuery") == false) {
            return null;
        }

        try {
            Field fieldName = queryClass.getDeclaredField("field");
            fieldName.setAccessible(true);

            String field = fieldName.get(query).toString();
            if (Objects.equals(field, fieldName.get(extraQuery)) == false) {
                return null;
            }

            Field lowerValue = queryClass.getDeclaredField("lowerValue");
            Field upperValue = queryClass.getDeclaredField("upperValue");
            lowerValue.setAccessible(true);
            upperValue.setAccessible(true);

            long q1LowerValue = lowerValue.getLong(query);
            long q1UpperValue = upperValue.getLong(query);
            long q2LowerValue = lowerValue.getLong(extraQuery);
            long q2UpperValue = upperValue.getLong(extraQuery);

            if (q1UpperValue < q2LowerValue || q2UpperValue < q1LowerValue) {
                return new MatchNoDocsQuery("Non-overlapping range queries");
            }

            return NumericDocValuesField.newSlowRangeQuery(
                field,
                Math.max(q1LowerValue, q2LowerValue),
                Math.min(q1UpperValue, q2UpperValue)
            );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            return null;
        }
    }
}
