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

package org.elasticsearch.search.aggregations;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/**
 * A Collector that can collect data in separate buckets.
 */
public abstract class BucketCollector implements Collector {

    public static final BucketCollector NO_OP_COLLECTOR = new BucketCollector() {

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext reader) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        @Override
        public void preCollection() throws IOException {
            // no-op
        }
        @Override
        public void postCollection() throws IOException {
            // no-op
        }
        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    };

    @Override
    public abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException;

    /**
     * Pre collection callback.
     */
    public abstract void preCollection() throws IOException;

    /**
     * Post-collection callback.
     */
    public abstract void postCollection() throws IOException;

}
