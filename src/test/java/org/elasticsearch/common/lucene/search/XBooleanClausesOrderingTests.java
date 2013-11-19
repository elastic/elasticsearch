/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class XBooleanClausesOrderingTests extends ElasticsearchLuceneTestCase {

    final static FilterClause must = new FilterClause(null, BooleanClause.Occur.MUST);
    final static FilterClause mustNot = new FilterClause(null, BooleanClause.Occur.MUST_NOT);
    final static FilterClause should = new FilterClause(null, BooleanClause.Occur.SHOULD);

    @Test
    public void testOrdering() throws Exception {
        List<XBooleanFilter.ResultClause> clauses = new ArrayList<XBooleanFilter.ResultClause>();
        XBooleanFilter.ResultClause resultClause1 = new XBooleanFilter.ResultClause(mockBits, null, must, new MockIterator(10));
        clauses.add(resultClause1);
        XBooleanFilter.ResultClause resultClause2 = new XBooleanFilter.ResultClause(null, null, must, new MockIterator(10));
        clauses.add(resultClause2);
        XBooleanFilter.ResultClause resultClause3 = new XBooleanFilter.ResultClause(mockBits, null, should, new MockIterator(5));
        clauses.add(resultClause3);
        XBooleanFilter.ResultClause resultClause4 = new XBooleanFilter.ResultClause(null, null, should, new MockIterator(5));
        clauses.add(resultClause4);

        Comparator<XBooleanFilter.ResultClause> comparator = new XBooleanFilter.AllClausesProcessor(null, null, -1).comparator();
        CollectionUtil.timSort(clauses, comparator);
        assertSame(resultClause4, clauses.get(0));
        assertSame(resultClause2, clauses.get(1));
        assertSame(resultClause3, clauses.get(2));
        assertSame(resultClause1, clauses.get(3));
    }

    @Test
    public void testOrdering_withMustNot() throws Exception {
        List<XBooleanFilter.ResultClause> clauses = new ArrayList<XBooleanFilter.ResultClause>();
        XBooleanFilter.ResultClause resultClause1 = new XBooleanFilter.ResultClause(mockBits, null, mustNot, new MockIterator(10));
        clauses.add(resultClause1);
        XBooleanFilter.ResultClause resultClause2 = new XBooleanFilter.ResultClause(null, null, mustNot, new MockIterator(10));
        clauses.add(resultClause2);
        XBooleanFilter.ResultClause resultClause3 = new XBooleanFilter.ResultClause(mockBits, null, must, new MockIterator(5));
        clauses.add(resultClause3);
        XBooleanFilter.ResultClause resultClause4 = new XBooleanFilter.ResultClause(null, null, should, new MockIterator(5));
        clauses.add(resultClause4);

        Comparator<XBooleanFilter.ResultClause> comparator = new XBooleanFilter.AllClausesProcessor(null, null, -1).comparator();
        CollectionUtil.timSort(clauses, comparator);
        assertSame(resultClause2, clauses.get(0));
        assertSame(resultClause4, clauses.get(1));
        assertSame(resultClause1, clauses.get(2));
        assertSame(resultClause3, clauses.get(3));
    }


    Bits mockBits = new Bits() {

        @Override
        public boolean get(int index) {
            return false;
        }

        @Override
        public int length() {
            return 0;
        }
    };


    class MockIterator extends DocIdSetIterator {

        final long cost;

        MockIterator(long cost) {
            this.cost = cost;
        }

        @Override
        public int docID() {
            return 0;
        }

        @Override
        public int nextDoc() throws IOException {
            return 0;
        }

        @Override
        public int advance(int target) throws IOException {
            return 0;
        }

        @Override
        public long cost() {
            return cost;
        }
    }


}
