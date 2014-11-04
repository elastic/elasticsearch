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

package org.elasticsearch.search.reducers.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;

import java.io.IOException;
import java.util.List;

public class InternalRange extends InternalBucketReducerAggregation implements Range {

    public static final Type TYPE = new Type("rangereducer");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalRange readResult(StreamInput in) throws IOException {
            InternalRange selections = new InternalRange();
            selections.readFrom(in);
            return selections;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public InternalRange() {
        super();
    }

    public InternalRange(String name, List<InternalSelection> selections) {
        super(name, selections);
    }

    @Override
    public Type type() {
        return TYPE;
    }
}
