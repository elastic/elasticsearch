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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@link TermsAggregator} when the field is some kind of decimal number like a float, double, or distance.
 */
public class DoubleTerms extends InternalMappedTerms<DoubleTerms, DoubleTerms.Bucket> {
    public static final String NAME = "dterms";

    static class Bucket extends InternalTerms.Bucket<Bucket> {
        private final double term;

        Bucket(double term, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                DocValueFormat format) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            term = in.readDouble();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeDouble(term);
        }

        @Override
        public String getKeyAsString() {
            return format.format(term).toString();
        }

        @Override
        public Object getKey() {
            return term;
        }

        @Override
        public Number getKeyAsNumber() {
            return term;
        }

        @Override
        public int compareKey(Bucket other) {
            return Double.compare(term, other.term);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), term);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term).toString());
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(term, ((Bucket) obj).term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), term);
        }
    }

    public DoubleTerms(String name, BucketOrder order, int requiredSize, long minDocCount,
            Map<String, Object> metadata, DocValueFormat format, int shardSize, boolean showTermDocCountError, long otherDocCount,
            List<Bucket> buckets, long docCountError) {
        super(name, order, requiredSize, minDocCount, metadata, format, shardSize, showTermDocCountError,
                otherDocCount, buckets, docCountError);
    }

    /**
     * Read from a stream.
     */
    public DoubleTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public DoubleTerms create(List<Bucket> buckets) {
        return new DoubleTerms(name, order, requiredSize, minDocCount, metadata, format, shardSize,
                showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.term, prototype.docCount, aggregations, prototype.showDocCountError, prototype.docCountError,
                prototype.format);
    }

    @Override
    protected DoubleTerms create(String name, List<Bucket> buckets, long docCountError, long otherDocCount) {
        return new DoubleTerms(name, order, requiredSize, minDocCount, getMetadata(), format,
                shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        boolean promoteToDouble = false;
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof LongTerms && ((LongTerms) agg).format == DocValueFormat.RAW) {
                /*
                 * this terms agg mixes longs and doubles, we must promote longs to doubles to make the internal aggs
                 * compatible
                 */
                promoteToDouble = true;
                break;
            }
        }
        if (promoteToDouble == false) {
            return super.reduce(aggregations, reduceContext);
        }
        List<InternalAggregation> newAggs = new ArrayList<>(aggregations.size());
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof LongTerms) {
                DoubleTerms dTerms = LongTerms.convertLongTermsToDouble((LongTerms) agg, format);
                newAggs.add(dTerms);
            } else {
                newAggs.add(agg);
            }
        }
        return newAggs.get(0).reduce(newAggs, reduceContext);
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, DoubleTerms.Bucket prototype) {
        return new Bucket(prototype.term, docCount, aggs, prototype.showDocCountError, docCountError, format);
    }
}
