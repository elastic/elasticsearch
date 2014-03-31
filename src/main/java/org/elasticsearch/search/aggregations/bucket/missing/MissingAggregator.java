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
package org.elasticsearch.search.aggregations.bucket.missing;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

/**
 *
 */
public class MissingAggregator extends SingleBucketAggregator {

    private final ValuesSource valuesSource;
    private BytesValues values;

    public MissingAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                             AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        this.valuesSource = valuesSource;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        if (valuesSource != null) {
            values = valuesSource.bytesValues();
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        if (valuesSource == null || values.setDocument(doc) == 0) {
            collectBucket(doc, owningBucketOrdinal);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalMissing(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMissing(name, 0, buildEmptySubAggregations());
    }

    public static class Factory extends ValuesSourceAggregatorFactory {

        public Factory(String name, ValuesSourceConfig valueSourceConfig) {
            super(name, InternalMissing.TYPE.name(), valueSourceConfig);
        }

        @Override
        protected MissingAggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new MissingAggregator(name, factories, null, aggregationContext, parent);
        }

        @Override
        protected MissingAggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new MissingAggregator(name, factories, valuesSource, aggregationContext, parent);
        }
    }

}


