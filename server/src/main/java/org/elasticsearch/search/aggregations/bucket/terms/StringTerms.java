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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@link TermsAggregator} when the field is a String.
 */
public class StringTerms extends InternalMappedTerms<StringTerms, StringTerms.Bucket> {
    public static final String NAME = "sterms";
    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        BytesRef termBytes;

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                DocValueFormat format) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.termBytes = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            termBytes = in.readBytesRef();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
        }

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        // this method is needed for scripted numeric aggs
        @Override
        public Number getKeyAsNumber() {
            /*
             * If the term is a long greater than 2^52 then parsing as a double would lose accuracy. Therefore, we first parse as a long and
             * if this fails then we attempt to parse the term as a double.
             */
            try {
                return Long.parseLong(termBytes.utf8ToString());
            } catch (final NumberFormatException ignored) {
                return Double.parseDouble(termBytes.utf8ToString());
            }
        }

        @Override
        public String getKeyAsString() {
            return format.format(termBytes).toString();
        }

        @Override
        public int compareKey(Bucket other) {
            return termBytes.compareTo(other.termBytes);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(termBytes, ((Bucket) obj).termBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), termBytes);
        }
    }

    public StringTerms(String name, BucketOrder order, int requiredSize, long minDocCount,
            Map<String, Object> metadata, DocValueFormat format, int shardSize, boolean showTermDocCountError, long otherDocCount,
            List<Bucket> buckets, long docCountError) {
        super(name, order, requiredSize, minDocCount, metadata, format,
                shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    /**
     * Read from a stream.
     */
    public StringTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public StringTerms create(List<Bucket> buckets) {
        return new StringTerms(name, order, requiredSize, minDocCount, metadata, format, shardSize,
                showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.termBytes, prototype.docCount, aggregations, prototype.showDocCountError, prototype.docCountError,
                prototype.format);
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, StringTerms.Bucket prototype) {
        return new Bucket(prototype.termBytes, docCount, aggs, prototype.showDocCountError, docCountError, format);
    }

    @Override
    protected StringTerms create(String name, List<Bucket> buckets, long docCountError, long otherDocCount) {
        return new StringTerms(name, order, requiredSize, minDocCount, getMetadata(), format, shardSize,
                showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }
}
