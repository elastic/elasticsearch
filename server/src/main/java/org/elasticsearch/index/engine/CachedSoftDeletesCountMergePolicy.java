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

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A {@link FilterMergePolicy} that caches the number of deletes to be merge within a given {@code timeToLiveInMillis} duration so we
 * can reduce the load of each refresh. Although returning a cached (maybe outdated) delete count to merge can delay reclaiming merges,
 * it should not be an issue because if a segment is selected, a {@link MergePolicy} will re-calculate the exact documents to be merged.
 * Moreover, all cached values will be invalidated if a force-merge is triggered so a force-merge is performed without any delay.
 */
final class CachedSoftDeletesCountMergePolicy extends FilterMergePolicy {
    private final Cache<SegmentCommitInfo, Integer> cacheOfNumDeletesToMerge;

    CachedSoftDeletesCountMergePolicy(MergePolicy in, TimeValue timeToLive) {
        super(in);
        this.cacheOfNumDeletesToMerge = CacheBuilder.<SegmentCommitInfo, Integer>builder()
            .setExpireAfterWrite(timeToLive)
            .setMaximumWeight(200) // we cache at most 200 segments - that's plenty
            .build();
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                               Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                                               MergeContext mergeContext) throws IOException {
        invalidateCaches();
        return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        invalidateCaches();
        return super.findForcedDeletesMerges(segmentInfos, mergeContext);
    }

    private void invalidateCaches() {
        cacheOfNumDeletesToMerge.invalidateAll();
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) {
        try {
            return cacheOfNumDeletesToMerge.computeIfAbsent(info, key -> super.numDeletesToMerge(info, delCount, readerSupplier));
        } catch (ExecutionException ex) {
            throw new ElasticsearchException("failed to compute numDeletesToMerge", ex);
        }
    }

    @Override
    protected long size(SegmentCommitInfo info, MergeContext context) throws IOException {
       return super.size(info, new MergeContext() {
           @Override
           public int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
               Integer deletesToMerge = cacheOfNumDeletesToMerge.get(info); // let see if we have this segments cached
               if (deletesToMerge != null) {
                   return deletesToMerge.intValue();
               } else {
                   return context.numDeletesToMerge(info);
               }
           }

           @Override
           public int numDeletedDocs(SegmentCommitInfo info) {
               return context.numDeletedDocs(info);
           }

           @Override
           public InfoStream getInfoStream() {
               return context.getInfoStream();
           }

           @Override
           public Set<SegmentCommitInfo> getMergingSegments() {
               return context.getMergingSegments();
           }
       });
    }
}
