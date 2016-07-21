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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude.OrdinalsFilter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeSet;

public class IncludeExcludeTests extends ESTestCase {

    public void testEmptyTermsWithOrds() throws IOException {
        IncludeExclude inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
                null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());

        inexcl = new IncludeExclude(
                null,
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());
    }

    public void testSingleTermWithOrds() throws IOException {
        RandomAccessOrds ords = new RandomAccessOrds() {

            boolean consumed = true;

            @Override
            public void setDocument(int docID) {
                consumed = false;
            }

            @Override
            public long nextOrd() {
                if (consumed) {
                    return SortedSetDocValues.NO_MORE_ORDS;
                } else {
                    consumed = true;
                    return 0;
                }
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                assertEquals(0, ord);
                return new BytesRef("foo");
            }

            @Override
            public long getValueCount() {
                return 1;
            }

            @Override
            public long ordAt(int index) {
                return 0;
            }

            @Override
            public int cardinality() {
                return 1;
            }
        };
        IncludeExclude inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
                null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertTrue(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("bar"))),
                null);
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
                null, // means everything included
                new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));
    }

}
