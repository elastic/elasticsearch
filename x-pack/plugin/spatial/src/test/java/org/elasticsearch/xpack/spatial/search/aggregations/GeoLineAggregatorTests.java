/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.mockito.internal.matchers.ArrayEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class GeoLineAggregatorTests extends AggregatorTestCase {

    public void testSomething() throws IOException {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder()
            .setFieldName("value_field")
            .build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("sort_field").build();
        GeoLineAggregationBuilder aggregationBuilder = new GeoLineAggregationBuilder("_name")
            .value(valueConfig)
            .sort(sortConfig);

        int numPoints = randomIntBetween(1, 10000);
        int arrayLength = randomIntBetween(numPoints, numPoints + 1000);
        long[] points = new long[arrayLength];
        double[] sortValues = new double[arrayLength];
        for (int i = 0; i < numPoints; i++) {
            Point point = GeometryTestUtils.randomPoint(false);
            int encodedLat = GeoEncodingUtils.encodeLatitude(point.getLat());
            int encodedLon = GeoEncodingUtils.encodeLongitude(point.getLon());
            long lonLat = (((long) encodedLon) << 32) | (encodedLat & 0xffffffffL);
            points[i] = lonLat;
            sortValues[i] = i;
        }

        InternalGeoLine geoLine = new InternalGeoLine("_name",
            Arrays.copyOf(points, arrayLength), Arrays.copyOf(sortValues, arrayLength),
            numPoints, null, null);

        for (int i = 0; i < randomIntBetween(1, numPoints); i++) {
            int idx1 = randomIntBetween(0, numPoints);
            int idx2 = randomIntBetween(0, numPoints);
            final long tmpPoint = points[idx1];
            points[idx1] = points[idx2];
            points[idx2] = tmpPoint;
            final double tmpSortValue = sortValues[idx1];
            sortValues[idx1] = sortValues[idx2];
            sortValues[idx2] = tmpSortValue;
        }


        testCase(new MatchAllDocsQuery(), aggregationBuilder, iw -> {
            for (int i = 0; i < numPoints; i++) {
                int x = (int) points[i] >> 32;
                int y = (int) points[i];
                iw.addDocument(Arrays.asList(new LatLonDocValuesField("value_field",
                        GeoEncodingUtils.decodeLatitude(y),
                        GeoEncodingUtils.decodeLongitude(x)),
                    new SortedNumericDocValuesField("sort_field", NumericUtils.doubleToSortableLong(sortValues[i]))));
            }
        }, actualGeoLine -> {
            assertThat(actualGeoLine.length(), equalTo(geoLine.length()));
            for (int i = 0; i < geoLine.length(); i++) {
                assertThat(GeoEncodingUtils.decodeLongitude((int) actualGeoLine.line()[i]),
                    equalTo(GeoEncodingUtils.decodeLongitude((int) geoLine.line()[i])));
                assertThat(GeoEncodingUtils.decodeLatitude((int) actualGeoLine.line()[i] << 32),
                    equalTo(GeoEncodingUtils.decodeLatitude((int) geoLine.line()[i] << 32)));
            }
        });
    }

    private void testCase(Query query, GeoLineAggregationBuilder aggregationBuilder,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoLine> verify) throws IOException {
        testCase(query, aggregationBuilder, buildIndex, verify, NumberFieldMapper.NumberType.LONG);
    }

    private void testCase(Query query, GeoLineAggregationBuilder aggregationBuilder,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoLine> verify,
                          NumberFieldMapper.NumberType fieldNumberType) throws IOException {

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        try {
            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
            fieldType.setName("value_field");
            fieldType.setHasDocValues(true);

            MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(fieldNumberType);
            fieldType2.setName("sort_field");
            fieldType2.setHasDocValues(true);

            GeoLineAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType, fieldType2);
            aggregator.preCollection();
            indexSearcher.search(query, aggregator);
            aggregator.postCollection();
            verify.accept((InternalGeoLine) aggregator.buildAggregation(0L));
        } finally {
            indexReader.close();
            directory.close();
        }
    }
}
