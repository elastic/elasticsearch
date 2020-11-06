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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A refresh listener that tracks the amount of memory used by segments in the accounting circuit breaker.
 */
final class RamAccountingRefreshListener implements BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> {

    private final CircuitBreakerService breakerService;

    RamAccountingRefreshListener(CircuitBreakerService breakerService) {
        this.breakerService = breakerService;
    }

    @Override
    public void accept(ElasticsearchDirectoryReader reader, ElasticsearchDirectoryReader previousReader) {
        final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.ACCOUNTING);

        // Construct a list of the previous segment readers, we only want to track memory used
        // by new readers, so these will be exempted from the circuit breaking accounting.
        //
        // The Core CacheKey is used as the key for the set so that deletions still keep the correct
        // accounting, as using the Reader or Reader's CacheKey causes incorrect accounting.
        final Set<IndexReader.CacheKey> prevReaders;
        if (previousReader == null) {
            prevReaders = Collections.emptySet();
        } else {
            final List<LeafReaderContext> previousReaderLeaves = previousReader.leaves();
            prevReaders = new HashSet<>(previousReaderLeaves.size());
            for (LeafReaderContext lrc : previousReaderLeaves) {
                prevReaders.add(Lucene.segmentReader(lrc.reader()).getCoreCacheHelper().getKey());
            }
        }

        for (LeafReaderContext lrc : reader.leaves()) {
            final SegmentReader segmentReader = Lucene.segmentReader(lrc.reader());
            // don't add the segment's memory unless it is not referenced by the previous reader
            // (only new segments)
            if (prevReaders.contains(segmentReader.getCoreCacheHelper().getKey()) == false) {
                final long ramBytesUsed = segmentReader.ramBytesUsed();
                // add the segment memory to the breaker (non-breaking)
                breaker.addWithoutBreaking(ramBytesUsed);
                // and register a listener for when the segment is closed to decrement the
                // breaker accounting
                segmentReader.getCoreCacheHelper().addClosedListener(k -> breaker.addWithoutBreaking(-ramBytesUsed));
            }
        }
    }
}
