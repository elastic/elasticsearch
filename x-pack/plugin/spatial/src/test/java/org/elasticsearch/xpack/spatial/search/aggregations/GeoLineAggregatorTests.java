/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class GeoLineAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new SpatialPlugin());
    }

    public void testSomething() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("value_field")
            .build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name")
            .value(valueConfig)
            .sort(sortConfig);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name")
            .field("group_id")
            .subAggregation(lineAggregationBuilder);

        int numGroups = randomIntBetween(1, 3);
        Map<String, InternalGeoLine> lines = new HashMap<>(numGroups);
        Map<Integer, long[]> indexedPoints = new HashMap<>(numGroups);
        Map<Integer, double[]> indexedSortValues = new HashMap<>(numGroups);
        for (int groupOrd = 0; groupOrd < numGroups; groupOrd++) {
            int numPoints = randomIntBetween(2, 20000);
            boolean complete = numPoints <= 10000;
            int arrayLength = randomIntBetween(numPoints, numPoints);
            long[] points = new long[arrayLength];
            double[] sortValues = new double[arrayLength];
            for (int i = 0; i < numPoints; i++) {
                Point point = GeometryTestUtils.randomPoint(false);
                int encodedLat = GeoEncodingUtils.encodeLatitude(point.getLat());
                int encodedLon = GeoEncodingUtils.encodeLongitude(point.getLon());
                long lonLat = (((long) encodedLon) << 32) | encodedLat & 0xffffffffL;
                points[i] = lonLat;
                sortValues[i] = i;
            }
            lines.put(String.valueOf(groupOrd), new InternalGeoLine("_name",
                Arrays.copyOf(points, arrayLength), Arrays.copyOf(sortValues, arrayLength), numPoints, null, complete));

            for (int i = 0; i < randomIntBetween(1, numPoints); i++) {
                int idx1 = randomIntBetween(0, numPoints - 1);
                int idx2 = randomIntBetween(0, numPoints - 1);
                final long tmpPoint = points[idx1];
                points[idx1] = points[idx2];
                points[idx2] = tmpPoint;
                final double tmpSortValue = sortValues[idx1];
                sortValues[idx1] = sortValues[idx2];
                sortValues[idx2] = tmpSortValue;
            }
            indexedPoints.put(groupOrd, points);
            indexedSortValues.put(groupOrd, sortValues);
        }


        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            for (int group = 0; group < numGroups; group++) {
                long[] points = indexedPoints.get(group);
                double[] sortValues = indexedSortValues.get(group);
                for (int i = 0; i < points.length; i++) {
                    int x = (int) (points[i] >> 32);
                    int y = (int) points[i];
                    iw.addDocument(Arrays.asList(new LatLonDocValuesField("value_field",
                            GeoEncodingUtils.decodeLatitude(y),
                            GeoEncodingUtils.decodeLongitude(x)),
                        new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(sortValues[i])),
                        new SortedDocValuesField("group_id", new BytesRef(String.valueOf(group)))));
                }
            }
        }, terms -> {
            for (Terms.Bucket bucket : terms.getBuckets()) {
                InternalGeoLine expectedGeoLine = lines.get(bucket.getKeyAsString());
                assertThat(bucket.getDocCount(), equalTo((long) expectedGeoLine.length()));
                InternalGeoLine geoLine = bucket.getAggregations().get("_name");
                assertThat(geoLine.isComplete(), equalTo(expectedGeoLine.isComplete()));
                //assertArrayEquals(expectedGeoLine.line(), geoLine.line());
            }
        });
    }

    private void testCase(Query query, TermsAggregationBuilder aggregationBuilder,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<Terms> verify) throws IOException {
        testCase(query, aggregationBuilder, buildIndex, verify, NumberFieldMapper.NumberType.LONG);
    }

    private void testCase(Query query, TermsAggregationBuilder aggregationBuilder,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<Terms> verify,
                          NumberFieldMapper.NumberType fieldNumberType) throws IOException {

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        try {
            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("value_field");
            MappedFieldType groupFieldType = new KeywordFieldMapper.KeywordFieldType("group_id");
            MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("sort_field", fieldNumberType);

            Terms terms = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder,
                fieldType, fieldType2, groupFieldType);
            verify.accept(terms);
        } finally {
            indexReader.close();
            directory.close();
        }
    }
}
