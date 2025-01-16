/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalGeoDistance extends InternalRange<InternalGeoDistance.Bucket, InternalGeoDistance> {
    public static final Factory FACTORY = new Factory();

    static class Bucket extends InternalRange.Bucket {

        Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations) {
            super(key, from, to, docCount, aggregations, DocValueFormat.RAW);
        }

    }

    public static class Factory extends InternalRange.Factory<InternalGeoDistance.Bucket, InternalGeoDistance> {
        @Override
        public ValuesSourceType getValueSourceType() {
            return CoreValuesSourceType.GEOPOINT;
        }

        @Override
        public InternalGeoDistance create(
            String name,
            List<Bucket> ranges,
            DocValueFormat format,
            boolean keyed,
            Map<String, Object> metadata
        ) {
            return new InternalGeoDistance(name, ranges, keyed, metadata);
        }

        @Override
        public InternalGeoDistance create(List<Bucket> ranges, InternalGeoDistance prototype) {
            return new InternalGeoDistance(prototype.name, ranges, prototype.keyed, prototype.metadata);
        }

        @Override
        public Bucket createBucket(
            String key,
            double from,
            double to,
            long docCount,
            InternalAggregations aggregations,
            DocValueFormat format
        ) {
            return new Bucket(key, from, to, docCount, aggregations);
        }

        @Override
        public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
            return new Bucket(
                prototype.getKey(),
                ((Number) prototype.getFrom()).doubleValue(),
                ((Number) prototype.getTo()).doubleValue(),
                prototype.getDocCount(),
                aggregations
            );
        }
    }

    public InternalGeoDistance(String name, List<Bucket> ranges, boolean keyed, Map<String, Object> metadata) {
        super(name, ranges, DocValueFormat.RAW, keyed, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalGeoDistance(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public InternalRange.Factory<Bucket, InternalGeoDistance> getFactory() {
        return FACTORY;
    }

    @Override
    public String getWriteableName() {
        return GeoDistanceAggregationBuilder.NAME;
    }
}
