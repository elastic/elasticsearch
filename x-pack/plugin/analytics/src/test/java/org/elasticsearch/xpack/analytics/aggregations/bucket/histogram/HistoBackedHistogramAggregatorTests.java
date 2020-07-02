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

package org.elasticsearch.xpack.analytics.aggregations.bucket.histogram;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singleton;

public class HistoBackedHistogramAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testHistograms() throws Exception {
        try (Directory dir = newDirectory();
                RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            w.addDocument(singleton(getDocValue(FIELD_NAME, new double[] {0, 1.2, 10, 12})));
            w.addDocument(singleton(getDocValue(FIELD_NAME, new double[] {5.3, 6, 6, 20})));
            w.addDocument(singleton(getDocValue(FIELD_NAME, new double[] {-10, 0.01, 10, 10, 30, 90})));

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                    .field(FIELD_NAME)
                    .interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));
                assertEquals(7, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(3, histogram.getBuckets().get(2).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(3, histogram.getBuckets().get(3).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    private BinaryDocValuesField getDocValue(String fieldName, double[] values) throws IOException {
        TDigest histogram = new TDigestState(100.0); //default
        for (double value : values) {
            histogram.add(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        histogram.compress();
        Collection<Centroid> centroids = histogram.centroids();
        Iterator<Centroid> iterator = centroids.iterator();
        while ( iterator.hasNext()) {
            Centroid centroid = iterator.next();
            streamOutput.writeVInt(centroid.count());
            streamOutput.writeDouble(centroid.mean());
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // Note: this is the same list as Core, plus Analytics
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE,
            AnalyticsValuesSourceType.HISTOGRAM
        );
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new HistogramAggregationBuilder("_name").field(fieldName);
    }

    private MappedFieldType defaultFieldType(String fieldName) {
        return new HistogramFieldMapper.HistogramFieldType(fieldName, true, Collections.emptyMap());
    }

}
