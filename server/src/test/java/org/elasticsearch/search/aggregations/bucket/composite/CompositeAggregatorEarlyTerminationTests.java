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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.search.sort.SortOrder;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Suppress AssertingCodec because it doesn't work with {@link DocValues#unwrapSingleton} that is used in this test. */
@LuceneTestCase.SuppressCodecs("Asserting")
public class CompositeAggregatorEarlyTerminationTests extends CompositeAggregatorTestCase {
    public void testEarlyTermination() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("keyword", "a", "long", 100L, "foo", "bar"),
                createDocument("keyword", "c", "long", 100L, "foo", "bar"),
                createDocument("keyword", "a", "long", 0L, "foo", "bar"),
                createDocument("keyword", "d", "long", 10L, "foo", "bar"),
                createDocument("keyword", "b", "long", 10L, "foo", "bar"),
                createDocument("keyword", "c", "long", 10L, "foo", "bar"),
                createDocument("keyword", "e", "long", 100L, "foo", "bar"),
                createDocument("keyword", "e", "long", 10L, "foo", "bar")
            )
        );

        executeTestCase(true, false, new TermQuery(new Term("foo", "bar")),
            dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        new TermsValuesSourceBuilder("keyword").field("keyword"),
                        new TermsValuesSourceBuilder("long").field("long")
                    )).aggregateAfter(createAfterKey("keyword", "b", "long", 10L)).size(2),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=c, long=100}", result.afterKey().toString());
                assertEquals("{keyword=c, long=10}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=c, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertTrue(result.isTerminatedEarly());
            }
        );

        // source field and index sorting config have different order
        executeTestCase(true, false, new TermQuery(new Term("foo", "bar")),
            dataset,
            () ->
                new CompositeAggregationBuilder("name",
                    Arrays.asList(
                        // reverse source order
                        new TermsValuesSourceBuilder("keyword").field("keyword").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("long").field("long").order(SortOrder.DESC)
                    )
                ).aggregateAfter(createAfterKey("keyword", "c", "long", 10L)).size(2),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{keyword=a, long=100}", result.afterKey().toString());
                assertEquals("{keyword=b, long=10}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(0).getDocCount());
                assertEquals("{keyword=a, long=100}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(1).getDocCount());
                assertTrue(result.isTerminatedEarly());
            }
        );
    }
}
