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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 * Results of a date_historgram aggregation.
 */
public class InternalDateHistogram {

    public static final Factory HISTOGRAM_FACTORY = new Factory();
    static final Type TYPE = new Type("date_histogram");

    static class Bucket extends InternalHistogram.Bucket {
        Bucket(long key, long docCount, InternalAggregations aggregations, boolean keyed, DocValueFormat formatter,
                InternalHistogram.Factory<Bucket> factory) {
            super(key, docCount, keyed, formatter, factory, aggregations);
        }

        /**
         * Read from a stream.
         */
        Bucket(StreamInput in, boolean keyed, DocValueFormat formatter, InternalHistogram.Factory<Bucket> factory) throws IOException {
            super(in, keyed, formatter, factory);
        }

        @Override
        public String getKeyAsString() {
            return format.format(key);
        }

        @Override
        public DateTime getKey() {
            return new DateTime(key, DateTimeZone.UTC);
        }

        @Override
        public String toString() {
            return getKeyAsString();
        }
    }

    static class Factory extends InternalHistogram.Factory<InternalDateHistogram.Bucket> {

        Factory() {
        }

        @Override
        public Type type() {
            return TYPE;
        }

        @Override
        public ValueType valueType() {
            return ValueType.DATE;
        }

        @Override
        public InternalDateHistogram.Bucket createBucket(InternalAggregations aggregations, InternalDateHistogram.Bucket prototype) {
            return new Bucket(prototype.key, prototype.docCount, aggregations, prototype.getKeyed(), prototype.format, this);
        }

        @Override
        public InternalDateHistogram.Bucket createBucket(Object key, long docCount, InternalAggregations aggregations, boolean keyed,
                DocValueFormat formatter) {
            if (key instanceof Number) {
                return new Bucket(((Number) key).longValue(), docCount, aggregations, keyed, formatter, this);
            } else if (key instanceof DateTime) {
                return new Bucket(((DateTime) key).getMillis(), docCount, aggregations, keyed, formatter, this);
            } else {
                throw new AggregationExecutionException("Expected key of type Number or DateTime but got [" + key + "]");
            }
        }

        @Override
        protected Bucket readBucket(StreamInput in, boolean keyed, DocValueFormat format) throws IOException {
            return new Bucket(in, keyed, format, this);
        }
    }

    private InternalDateHistogram() {}
}
