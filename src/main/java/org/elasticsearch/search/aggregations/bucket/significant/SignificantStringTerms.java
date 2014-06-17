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
package org.elasticsearch.search.aggregations.bucket.significant;

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
public class SignificantStringTerms extends InternalSignificantTerms {

    public static final InternalAggregation.Type TYPE = new Type("significant_terms", "sigsterms");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public SignificantStringTerms readResult(StreamInput in) throws IOException {
            SignificantStringTerms buckets = new SignificantStringTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket extends InternalSignificantTerms.Bucket {

        BytesRef termBytes;


        public Bucket(BytesRef term, long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations);
            this.termBytes = term;
        }

        @Override
        public Text getKeyAsText() {
            return new BytesText(new BytesArray(termBytes));
        }

        @Override
        public Number getKeyAsNumber() {
            // this method is needed for scripted numeric aggregations
            return Double.parseDouble(termBytes.utf8ToString());
        }

        @Override
        int compareTerm(SignificantTerms.Bucket other) {
            return BytesRef.getUTF8SortedAsUnicodeComparator().compare(termBytes, ((Bucket) other).termBytes);
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        Bucket newBucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations) {
            return new Bucket(termBytes, subsetDf, subsetSize, supersetDf, supersetSize, aggregations);
        }
    }

    SignificantStringTerms() {} // for serialization

    public SignificantStringTerms(long subsetSize, long supersetSize, String name, int requiredSize,
            long minDocCount, Collection<InternalSignificantTerms.Bucket> buckets) {
        super(subsetSize, supersetSize, name, requiredSize, minDocCount, buckets);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    InternalSignificantTerms newAggregation(long subsetSize, long supersetSize,
            List<InternalSignificantTerms.Bucket> buckets) {
        return new SignificantStringTerms(subsetSize, supersetSize, getName(), requiredSize, minDocCount, buckets);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
        this.subsetSize = in.readVLong();
        this.supersetSize = in.readVLong();
        int size = in.readVInt();
        List<InternalSignificantTerms.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            BytesRef term = in.readBytesRef();
            long subsetDf = in.readVLong();
            long supersetDf = in.readVLong();
            buckets.add(new Bucket(term, subsetDf, subsetSize, supersetDf, supersetSize, InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeVLong(subsetSize);
        out.writeVLong(supersetSize);
        out.writeVInt(buckets.size());
        for (InternalSignificantTerms.Bucket bucket : buckets) {
            out.writeBytesRef(((Bucket) bucket).termBytes);
            out.writeVLong(((Bucket) bucket).subsetDf);
            out.writeVLong(((Bucket) bucket).supersetDf);
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("doc_count", subsetSize);
        builder.startArray(CommonFields.BUCKETS);
        for (InternalSignificantTerms.Bucket bucket : buckets) {
            //There is a condition (presumably when only one shard has a bucket?) where reduce is not called
            // and I end up with buckets that contravene the user's min_doc_count criteria in my reducer
            if (bucket.subsetDf >= minDocCount) {
                builder.startObject();
                builder.utf8Field(CommonFields.KEY, ((Bucket) bucket).termBytes);
                builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
                builder.field("score", bucket.score);
                builder.field("bg_count", bucket.supersetDf);
                ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
                builder.endObject();
            }
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
