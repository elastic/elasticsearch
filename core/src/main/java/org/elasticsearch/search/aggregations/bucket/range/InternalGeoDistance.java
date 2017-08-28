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
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalGeoDistance extends InternalRange<InternalGeoDistance.Bucket, InternalGeoDistance> {
    public static final Factory FACTORY = new Factory();

    static class Bucket extends InternalRange.Bucket {

        Bucket(boolean keyed) {
            super(keyed, DocValueFormat.RAW);
        }

        Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations, boolean keyed) {
            this(key, from, to, docCount, new InternalAggregations(aggregations), keyed);
        }

        Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed) {
            super(key, from, to, docCount, aggregations, keyed, DocValueFormat.RAW);
        }

        @Override
        protected InternalRange.Factory<Bucket, ?> getFactory() {
            return FACTORY;
        }

        boolean keyed() {
            return keyed;
        }
    }

    public static class Factory extends InternalRange.Factory<InternalGeoDistance.Bucket, InternalGeoDistance> {
        @Override
        public ValuesSourceType getValueSourceType() {
            return ValuesSourceType.GEOPOINT;
        }

        @Override
        public ValueType getValueType() {
            return ValueType.GEOPOINT;
        }

        @Override
        public InternalGeoDistance create(String name, List<Bucket> ranges, DocValueFormat format, boolean keyed,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            return new InternalGeoDistance(name, ranges, keyed, pipelineAggregators, metaData);
        }

        @Override
        public InternalGeoDistance create(List<Bucket> ranges, InternalGeoDistance prototype) {
            return new InternalGeoDistance(prototype.name, ranges, prototype.keyed, prototype.pipelineAggregators(),
                    prototype.metaData);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations, boolean keyed,
                DocValueFormat format) {
            return new Bucket(key, from, to, docCount, aggregations, keyed);
        }

        @Override
        public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
            return new Bucket(prototype.getKey(), ((Number) prototype.getFrom()).doubleValue(), ((Number) prototype.getTo()).doubleValue(),
                    prototype.getDocCount(), aggregations, prototype.getKeyed());
        }
    }

    public InternalGeoDistance(String name, List<Bucket> ranges, boolean keyed,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, ranges, DocValueFormat.RAW, keyed, pipelineAggregators, metaData);
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
