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

package org.elasticsearch.search.reducers.bucket.union;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalUnion extends InternalBucketReducerAggregation implements Union {

    public static final Type TYPE = new Type("union");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalUnion readResult(StreamInput in) throws IOException {
            InternalUnion selections = new InternalUnion();
            selections.readFrom(in);
            return selections;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new InternalSelection();
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public InternalUnion() {
        super();
    }

    public InternalUnion(String name, List<InternalSelection> selections, Map<String, Object> metaData) {
        super(name, selections, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
    }

}
