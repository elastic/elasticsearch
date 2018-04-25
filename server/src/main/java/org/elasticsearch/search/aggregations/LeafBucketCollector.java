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

import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Per-leaf bucket collector.
 */
public abstract class LeafBucketCollector implements LeafCollector {

    public static final LeafBucketCollector NO_OP_COLLECTOR = new LeafBucketCollector() {
        @Override
        public void setScorer(Scorer arg0) throws IOException {
            // no-op
        }
        @Override
        public void collect(int doc, long bucket) {
            // no-op
        }
    };

    public static LeafBucketCollector wrap(Iterable<LeafBucketCollector> collectors) {
        final Stream<LeafBucketCollector> actualCollectors =
                StreamSupport.stream(collectors.spliterator(), false).filter(c -> c != NO_OP_COLLECTOR);
        final LeafBucketCollector[] colls = actualCollectors.toArray(size -> new LeafBucketCollector[size]);
        switch (colls.length) {
        case 0:
            return NO_OP_COLLECTOR;
        case 1:
            return colls[0];
        default:
            return new LeafBucketCollector() {

                @Override
                public void setScorer(Scorer s) throws IOException {
                    for (LeafBucketCollector c : colls) {
                        c.setScorer(s);
                    }
                }

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    for (LeafBucketCollector c : colls) {
                        c.collect(doc, bucket);
                    }
                }

            };
        }
    }

    /**
     * Collect the given doc in the given bucket.
     */
    public abstract void collect(int doc, long bucket) throws IOException;

    @Override
    public final void collect(int doc) throws IOException {
        collect(doc, 0);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        // no-op by default
    }
}
