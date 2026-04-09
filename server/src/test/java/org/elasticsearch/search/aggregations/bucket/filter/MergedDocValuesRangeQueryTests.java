/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class MergedDocValuesRangeQueryTests extends ESTestCase {

    public void testNotRangeQueries() {
        assertThat(
            MergedDocValuesRangeQuery.merge(LongPoint.newRangeQuery("field", 1, 4), new TermQuery(new Term("field", "foo"))),
            nullValue()
        );

        assertThat(
            MergedDocValuesRangeQuery.merge(NumericDocValuesField.newSlowRangeQuery("field", 1, 4), LongPoint.newRangeQuery("field", 2, 4)),
            nullValue()
        );

        assertThat(
            MergedDocValuesRangeQuery.merge(LongPoint.newRangeQuery("field", 2, 4), NumericDocValuesField.newSlowRangeQuery("field", 1, 4)),
            nullValue()
        );
    }

    public void testDifferentFields() {
        assertThat(
            MergedDocValuesRangeQuery.merge(
                NumericDocValuesField.newSlowRangeQuery("field1", 1, 4),
                NumericDocValuesField.newSlowRangeQuery("field2", 2, 4)
            ),
            nullValue()
        );
    }

    public void testNoOverlap() {
        assertThat(
            MergedDocValuesRangeQuery.merge(
                NumericDocValuesField.newSlowRangeQuery("field", 1, 4),
                NumericDocValuesField.newSlowRangeQuery("field", 6, 8)
            ),
            instanceOf(MatchNoDocsQuery.class)
        );
    }

    public void testOverlap() {
        assertThat(
            MergedDocValuesRangeQuery.merge(
                NumericDocValuesField.newSlowRangeQuery("field", 1, 6),
                NumericDocValuesField.newSlowRangeQuery("field", 4, 8)
            ),
            equalTo(NumericDocValuesField.newSlowRangeQuery("field", 4, 6))
        );

        assertThat(
            MergedDocValuesRangeQuery.merge(
                NumericDocValuesField.newSlowRangeQuery("field", 4, 8),
                NumericDocValuesField.newSlowRangeQuery("field", 1, 6)
            ),
            equalTo(NumericDocValuesField.newSlowRangeQuery("field", 4, 6))
        );

        assertThat(
            MergedDocValuesRangeQuery.merge(
                NumericDocValuesField.newSlowRangeQuery("field", 1, 8),
                NumericDocValuesField.newSlowRangeQuery("field", 4, 6)
            ),
            equalTo(NumericDocValuesField.newSlowRangeQuery("field", 4, 6))
        );

        assertThat(
            MergedDocValuesRangeQuery.merge(
                NumericDocValuesField.newSlowRangeQuery("field", 4, 6),
                NumericDocValuesField.newSlowRangeQuery("field", 1, 8)
            ),
            equalTo(NumericDocValuesField.newSlowRangeQuery("field", 4, 6))
        );
    }

}
