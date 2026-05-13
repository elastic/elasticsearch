/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class LeafQueryBuilderTests extends ESTestCase {

    public void testEstimateRamBytesEqualsRamBytesUsedForAccountable() {
        long expected = randomLongBetween(1L, 1_000_000L);
        Query accountable = new AccountableTestQuery(expected);
        assertEquals(expected, LeafQueryBuilder.estimateRamBytes(accountable));
    }

    public void testEstimateRamBytesFallsBackToShallowSizePlusFloorForNonAccountable() {
        Query nonAccountable = new TermQuery(new Term("field", "value"));
        long expected = RamUsageEstimator.shallowSizeOf(nonAccountable) + LeafQueryBuilder.LEAF_BASE_BYTES;
        assertEquals(expected, LeafQueryBuilder.estimateRamBytes(nonAccountable));
    }

    public void testNonAccountableEstimateIsAtLeastTheFloor() {
        Query nonAccountable = new TermQuery(new Term("field", "value"));
        assertThat(LeafQueryBuilder.estimateRamBytes(nonAccountable), greaterThanOrEqualTo(LeafQueryBuilder.LEAF_BASE_BYTES));
    }

    private static final class AccountableTestQuery extends Query implements Accountable {
        private final long ramBytes;

        AccountableTestQuery(long ramBytes) {
            this.ramBytes = ramBytes;
        }

        @Override
        public long ramBytesUsed() {
            return ramBytes;
        }

        @Override
        public String toString(String field) {
            return "AccountableTestQuery[" + ramBytes + "]";
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }
}
