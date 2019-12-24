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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

/** A range aggregation for data that is encoded in doc values using a binary representation. */
public final class InternalBinaryRange
        extends InternalMultiBucketAggregation<InternalBinaryRange, InternalBinaryRange.Bucket>
        implements Range {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Range.Bucket {

        private final transient DocValueFormat format;
        private final transient boolean keyed;
        private final String key;
        private final BytesRef from, to;
        private final long docCount;
        private final InternalAggregations aggregations;

        public Bucket(DocValueFormat format, boolean keyed, String key, BytesRef from, BytesRef to,
                long docCount, InternalAggregations aggregations) {
            this.format = format;
            this.keyed = keyed;
            this.key = key != null ? key : generateKey(from, to, format);
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        private static String generateKey(BytesRef from, BytesRef to, DocValueFormat format) {
            StringBuilder builder = new StringBuilder()
                .append(from == null ? "*" : format.format(from))
                .append("-")
                .append(to == null ? "*" : format.format(to));
            return builder.toString();
        }

        private static Bucket createFromStream(StreamInput in, DocValueFormat format, boolean keyed) throws IOException {
            String key = in.readString();
            BytesRef from = in.readBoolean() ? in.readBytesRef() : null;
            BytesRef to = in.readBoolean() ? in.readBytesRef() : null;
            long docCount = in.readLong();
            InternalAggregations aggregations = new InternalAggregations(in);

            return new Bucket(format, keyed, key, from, to, docCount, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeBoolean(from != null);
            if (from != null) {
                out.writeBytesRef(from);
            }
            out.writeBoolean(to != null);
            if (to != null) {
                out.writeBytesRef(to);
            }
            out.writeLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String key = this.key;
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
            if (from != null) {
                builder.field(CommonFields.FROM.getPreferredName(), getFrom());
            }
            if (to != null) {
                builder.field(CommonFields.TO.getPreferredName(), getTo());
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public Object getFrom() {
            return getFromAsString();
        }

        @Override
        public String getFromAsString() {
            return from == null ? null : format.format(from).toString();
        }

        @Override
        public Object getTo() {
            return getToAsString();
        }

        @Override
        public String getToAsString() {
            return to == null ? null : format.format(to).toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bucket bucket = (Bucket) o;

            if (docCount != bucket.docCount) return false;
            // keyed and format are ignored since they are already tested on the InternalBinaryRange object
            return Objects.equals(key, bucket.key) &&
                Objects.equals(from, bucket.from) &&
                Objects.equals(to, bucket.to) &&
                Objects.equals(aggregations, bucket.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, key, from, to, aggregations);
        }
    }

    protected final DocValueFormat format;
    protected final boolean keyed;
    private final List<Bucket> buckets;

    public InternalBinaryRange(String name, DocValueFormat format, boolean keyed, List<Bucket> buckets,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.format = format;
        this.keyed = keyed;
        this.buckets = buckets;
    }

    /**
     * Read from a stream.
     */
    public InternalBinaryRange(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        buckets = in.readList(stream -> Bucket.createFromStream(stream, format, keyed));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return IpRangeAggregationBuilder.NAME;
    }

    @Override
    public List<InternalBinaryRange.Bucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public InternalBinaryRange create(List<Bucket> buckets) {
        return new InternalBinaryRange(name, format, keyed, buckets, pipelineAggregators(), metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(format, keyed, prototype.key, prototype.from, prototype.to, prototype.docCount, aggregations);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        reduceContext.consumeBucketsAndMaybeBreak(buckets.size());
        long[] docCounts = new long[buckets.size()];
        InternalAggregations[][] aggs = new InternalAggregations[buckets.size()][];
        for (int i = 0; i < aggs.length; ++i) {
            aggs[i] = new InternalAggregations[aggregations.size()];
        }
        for (int i = 0; i < aggregations.size(); ++i) {
            InternalBinaryRange range = (InternalBinaryRange) aggregations.get(i);
            if (range.buckets.size() != buckets.size()) {
                throw new IllegalStateException("Expected [" + buckets.size() + "] buckets, but got [" + range.buckets.size() + "]");
            }
            for (int j = 0; j < buckets.size(); ++j) {
                Bucket bucket = range.buckets.get(j);
                docCounts[j] += bucket.docCount;
                aggs[j][i] = bucket.aggregations;
            }
        }
        List<Bucket> buckets = new ArrayList<>(this.buckets.size());
        for (int i = 0; i < this.buckets.size(); ++i) {
            Bucket b = this.buckets.get(i);
            buckets.add(new Bucket(format, keyed, b.key, b.from, b.to, docCounts[i],
                    InternalAggregations.reduce(Arrays.asList(aggs[i]), reduceContext)));
        }
        return new InternalBinaryRange(name, format, keyed, buckets, pipelineAggregators(), metaData);
    }

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregationsList = buckets.stream().map(bucket -> bucket.aggregations).collect(Collectors.toList());
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(aggs, buckets.get(0));
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder,
            Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (Bucket range : buckets) {
            range.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBinaryRange that = (InternalBinaryRange) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(format, that.format)
            && Objects.equals(keyed, that.keyed);
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, format, keyed);
    }
}
