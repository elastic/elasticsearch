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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class StringTerms extends InternalTerms {

    public static final InternalAggregation.Type TYPE = new Type("terms", "sterms");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public StringTerms readResult(StreamInput in) throws IOException {
            StringTerms buckets = new StringTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    public static class Bucket extends InternalTerms.Bucket {

        BytesRef termBytes;

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations) {
            super(docCount, aggregations);
            this.termBytes = term;
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        public Text getKeyAsText() {
            return new BytesText(new BytesArray(termBytes));
        }

        @Override
        public Number getKeyAsNumber() {
            // this method is needed for scripted numeric faceting
            return Double.parseDouble(termBytes.utf8ToString());
        }

        @Override
        int compareTerm(Terms.Bucket other) {
            return BytesRef.getUTF8SortedAsUnicodeComparator().compare(termBytes, ((Bucket) other).termBytes);
        }

        @Override
        Object getKeyAsObject() {
            return getKeyAsText();
        }

        @Override
        Bucket newBucket(long docCount, InternalAggregations aggs) {
            return new Bucket(termBytes, docCount, aggs);
        }
    }

    StringTerms() {} // for serialization

    public StringTerms(String name, InternalOrder order, int requiredSize, long minDocCount, Collection<InternalTerms.Bucket> buckets) {
        super(name, order, requiredSize, minDocCount, buckets);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected InternalTerms newAggregation(String name, List<InternalTerms.Bucket> buckets) {
        return new StringTerms(name, order, requiredSize, minDocCount, buckets);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.order = InternalOrder.Streams.readOrder(in);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        int size = in.readVInt();
        List<InternalTerms.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readBytesRef(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeVInt(buckets.size());
        for (InternalTerms.Bucket bucket : buckets) {
            out.writeBytesRef(((Bucket) bucket).termBytes);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.startArray(CommonFields.BUCKETS);
        for (InternalTerms.Bucket bucket : buckets) {
            builder.startObject();
            builder.field(CommonFields.KEY, ((Bucket) bucket).termBytes);
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
