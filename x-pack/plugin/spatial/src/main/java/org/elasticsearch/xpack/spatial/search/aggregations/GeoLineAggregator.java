/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.spatial.search.aggregations.GeoLineAggregationBuilder.GEO_POINT_FIELD;
import static org.elasticsearch.xpack.spatial.search.aggregations.GeoLineAggregationBuilder.SORT_FIELD;

/**
 * Metric Aggregation for computing the pearson product correlation coefficient between multiple fields
 **/
final class GeoLineAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    private final MultiValuesSource.AnyMultiValuesSource valuesSources;

    private ObjectArray<long[]> paths;
    private ObjectArray<double[]> sortValues;
    private IntArray idxs;

    GeoLineAggregator(String name, MultiValuesSource.AnyMultiValuesSource valuesSources, SearchContext context,
                      Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                      Map<String,Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSources = valuesSources;
        if (valuesSources != null) {
            paths = context.bigArrays().newObjectArray(1);
            sortValues = context.bigArrays().newObjectArray(1);
            idxs = context.bigArrays().newIntArray(1);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSources != null && valuesSources.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        MultiGeoPointValues docGeoPointValues = valuesSources.getGeoPointField(GEO_POINT_FIELD.getPreferredName(), ctx);
        SortedNumericDoubleValues docSortValues = valuesSources.getNumericField(SORT_FIELD.getPreferredName(), ctx);

        return new LeafBucketCollectorBase(sub, docGeoPointValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                paths = bigArrays.grow(paths, bucket + 1);
                if (docGeoPointValues.advanceExact(doc) && docSortValues.advanceExact(doc)) {
                    if (docSortValues.docValueCount() > 1) {
                        throw new AggregationExecutionException("Encountered more than one sort value for a " +
                            "single document. Use a script to combine multiple sort-values-per-doc into a single value.");
                    }
                    if (docGeoPointValues.docValueCount() > 1) {
                        throw new AggregationExecutionException("Encountered more than one geo_point value for a " +
                            "single document. Use a script to combine multiple geo_point-values-per-doc into a single value.");
                    }

                    // There should always be one weight if advanceExact lands us here, either
                    // a real weight or a `missing` weight
                    assert docSortValues.docValueCount() == 1;
                    assert docGeoPointValues.docValueCount() == 1;
                    final double sort = docSortValues.nextValue();
                    final GeoPoint point = docGeoPointValues.nextValue();

                    int idx = idxs.get(bucket);
                    long[] bucketLine = paths.get(bucket);
                    double[] sortVals = sortValues.get(bucket);
                    if (bucketLine == null) {
                        bucketLine = new long[10];
                    } else {
                        bucketLine = ArrayUtil.grow(bucketLine, idx + 1);
                    }


                    if (sortVals == null) {
                        sortVals = new double[10];
                    } else {
                        sortVals = ArrayUtil.grow(sortVals, idx + 1);
                    }

                    int encodedLat = GeoEncodingUtils.encodeLatitude(point.lat());
                    int encodedLon = GeoEncodingUtils.encodeLongitude(point.lon());
                    long lonLat = (((long) encodedLon) << 32) | (encodedLat & 0xffffffffL);

                    sortVals[idx] = sort;
                    bucketLine[idx] = lonLat;

                    paths.set(bucket, bucketLine);
                    sortValues.set(bucket, sortVals);
                    idxs.set(bucket, idx + 1);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null) {
            return buildEmptyAggregation();
        }
        long[] bucketLine = paths.get(bucket);
        double[] sortVals = sortValues.get(bucket);
        int length = idxs.get(bucket);
        new PathArraySorter(bucketLine, sortVals, length).sort();
        return new InternalGeoLine(name, bucketLine, sortVals, length, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoLine(name, null, null, 0, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(paths, idxs, sortValues);
    }
}
