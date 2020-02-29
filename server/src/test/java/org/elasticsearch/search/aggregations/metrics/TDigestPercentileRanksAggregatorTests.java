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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Iterator;


public class TDigestPercentileRanksAggregatorTests extends AggregatorTestCase {

    public void testEmpty() throws IOException {
        PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("my_agg", new double[]{0.5})
                .field("field")
                .method(PercentilesMethod.TDIGEST);
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        fieldType.setName("field");
        try (IndexReader reader = new MultiReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            PercentileRanks ranks = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
            Percentile rank = ranks.iterator().next();
            assertEquals(Double.NaN, rank.getPercent(), 0d);
            assertEquals(0.5, rank.getValue(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(((InternalTDigestPercentileRanks)ranks)));
        }
    }

    public void testSimple() throws IOException {
        try (Directory dir = newDirectory();
                RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] {3, 0.2, 10}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("my_agg", new double[]{0.1, 0.5, 12})
                    .field("field")
                    .method(PercentilesMethod.TDIGEST);
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                PercentileRanks ranks = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                Iterator<Percentile> rankIterator = ranks.iterator();
                Percentile rank = rankIterator.next();
                assertEquals(0.1, rank.getValue(), 0d);
                // TODO: Fix T-Digest: this assertion should pass but we currently get ~15
                // https://github.com/elastic/elasticsearch/issues/14851
                // assertThat(rank.getPercent(), Matchers.equalTo(0d));
                rank = rankIterator.next();
                assertEquals(0.5, rank.getValue(), 0d);
                assertThat(rank.getPercent(), Matchers.greaterThan(0d));
                assertThat(rank.getPercent(), Matchers.lessThan(100d));
                rank = rankIterator.next();
                assertEquals(12, rank.getValue(), 0d);
                // TODO: Fix T-Digest: this assertion should pass but we currently get ~59
                // https://github.com/elastic/elasticsearch/issues/14851
                // assertThat(rank.getPercent(), Matchers.equalTo(100d));
                assertFalse(rankIterator.hasNext());
                assertTrue(AggregationInspectionHelper.hasValue(((InternalTDigestPercentileRanks)ranks)));
            }
        }
    }

    public void testNullValues() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new PercentileRanksAggregationBuilder("my_agg", null).field("field").method(PercentilesMethod.TDIGEST));
        assertThat(e.getMessage(), Matchers.equalTo("[values] must not be null: [my_agg]"));
    }

    public void testEmptyValues() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new PercentileRanksAggregationBuilder("my_agg", new double[0]).field("field").method(PercentilesMethod.TDIGEST));

        assertThat(e.getMessage(), Matchers.equalTo("[values] must not be an empty array: [my_agg]"));
    }
}
