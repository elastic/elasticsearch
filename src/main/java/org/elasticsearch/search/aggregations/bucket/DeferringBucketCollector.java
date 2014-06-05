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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.FilteringBucketCollector;
import org.elasticsearch.search.aggregations.RecordingBucketCollector;
import org.elasticsearch.search.aggregations.RecordingPerReaderBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;

/**
 * Buffers the matches in a collect stream and can replay a subset of the collected buckets
 * to a deferred set of collectors.
 * The rationale for not bundling all this logic into {@link RecordingBucketCollector} is to allow
 * the possibility of alternative recorder impl choices while keeping the logic in here for
 * setting {@link AggregationContext}'s setNextReader method and preparing the appropriate choice 
 * of filtering logic for stream replay. These felt like agg-specific functions that should be kept away
 * from the {@link RecordingBucketCollector} impl which is concentrated on efficient storage of doc and bucket IDs  
 */
public class DeferringBucketCollector extends BucketCollector implements Releasable {
    
    private final BucketCollector deferred;
    private final RecordingBucketCollector recording;
    private final AggregationContext context;
    private FilteringBucketCollector filteredCollector;


    public DeferringBucketCollector (BucketCollector deferred, AggregationContext context) {
        this.deferred = deferred;
        this.recording = new RecordingPerReaderBucketCollector(context);
        this.context = context;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        recording.setNextReader(reader);
    }

    @Override
    public void collect(int docId, long bucketOrdinal) throws IOException {
        recording.collect(docId, bucketOrdinal);
    }

    @Override
    public void postCollection() throws IOException {
        recording.postCollection();
    }
    
    /**
     * Plays a selection of the data cached from previous collect calls to the
     * deferred collector.
     * 
     * @param survivingBucketOrds
     *            the valid bucket ords for which deferred collection should be
     *            attempted
     */
    public void prepareSelectedBuckets(long... survivingBucketOrds) {
        
        BucketCollector subs = new BucketCollector() {
            @Override
            public void setNextReader(AtomicReaderContext reader) {
                // Need to set AggregationContext otherwise ValueSources in aggs
                // don't read any values
              context.setNextReader(reader);
              deferred.setNextReader(reader);
            }

            @Override
            public void collect(int docId, long bucketOrdinal) throws IOException {
                deferred.collect(docId, bucketOrdinal);
            }

            @Override
            public void postCollection() throws IOException {
                deferred.postCollection();
            }

            @Override
            public void gatherAnalysis(BucketAnalysisCollector results, long bucketOrdinal) {
                deferred.gatherAnalysis(results, bucketOrdinal);
            }
        };

        filteredCollector = new FilteringBucketCollector(survivingBucketOrds, subs, context.bigArrays());
        try {
            recording.replayCollection(filteredCollector);
        } catch (IOException e) {
            throw new QueryPhaseExecutionException(context.searchContext(), "Failed to replay deferred set of matching docIDs", e);
        }
    }

    

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(recording, filteredCollector);
    }

    @Override
    public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal)  {
        filteredCollector.gatherAnalysis(analysisCollector, bucketOrdinal);
    }

}
