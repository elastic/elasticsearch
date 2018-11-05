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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A {@link FilterMergePolicy} that caches the number of deletes to be merge within a given {@code timeToLiveInMillis} duration so we
 * can reduce the load upon refresh. Although this cache may delay reclaiming segments, it should not be an issue because if a segment
 * is selected to merge, a {@link MergePolicy} will use {@link org.apache.lucene.index.MergePolicy.OneMerge#wrapForMerge(CodecReader)}
 * to calculate the exact documents to be merged. Moreover, all cached values will be invalidated if a force-merge is triggered.
 */
final class CachedDeleteCountMergePolicy extends FilterMergePolicy {
    private final Cache<SegmentCommitInfo, Integer> cacheOfNumDeletesToMerge;
    private final Cache<SegmentCommitInfo, Long> cacheOfSegmentSizes;

    CachedDeleteCountMergePolicy(MergePolicy in, TimeValue timeToLive) {
        super(in);
        this.cacheOfNumDeletesToMerge = CacheBuilder.<SegmentCommitInfo, Integer>builder()
            .setExpireAfterWrite(timeToLive).setMaximumWeight(5000)
            .build();
        this.cacheOfSegmentSizes = CacheBuilder.<SegmentCommitInfo, Long>builder()
            .setExpireAfterWrite(timeToLive).setMaximumWeight(5000)
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
        cacheOfSegmentSizes.invalidateAll();
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
    protected long size(SegmentCommitInfo info, MergeContext context) {
        try {
            return cacheOfSegmentSizes.computeIfAbsent(info, key -> super.size(info, context));
        } catch (ExecutionException ex) {
            throw new ElasticsearchException("failed to calculate segment size", ex);
        }
    }
}
