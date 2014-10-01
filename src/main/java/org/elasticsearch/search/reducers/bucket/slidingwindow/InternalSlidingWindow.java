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

package org.elasticsearch.search.reducers.bucket.slidingwindow;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;

import java.io.IOException;
import java.util.List;

public class InternalSlidingWindow extends InternalBucketReducerAggregation implements SlidingWindow {

    public static final Type TYPE = new Type("sliding_window");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalSlidingWindow readResult(StreamInput in) throws IOException {
            InternalSlidingWindow selections = new InternalSlidingWindow();
            selections.readFrom(in);
            return selections;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public InternalSlidingWindow() {
        super();
    }

    public InternalSlidingWindow(String name, List<InternalSelection> selections) {
        super(name, selections);
    }

    @Override
    public Type type() {
        return TYPE;
    }
}
