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

package org.apache.lucene.index;

import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link FilterMergePolicy} that interleaves eldest and newest segments picked by {@link MergePolicy#findForcedMerges}
 * and {@link MergePolicy#findForcedDeletesMerges}. This allows time-based indices, that usually have the eldest documents
 * first, to be efficient at finding the most recent documents too.
 */
public class ShuffleForcedMergePolicy extends FilterMergePolicy {
    private static final String SHUFFLE_MERGE_KEY = "es.shuffle_merge";

    public ShuffleForcedMergePolicy(MergePolicy in) {
        super(in);
    }

    /**
     * Return <code>true</code> if the provided reader was merged with interleaved segments.
     */
    public static boolean isInterleavedSegment(LeafReader reader) {
        SegmentReader segReader = Lucene.segmentReader(reader);
        return segReader.getSegmentInfo().info.getDiagnostics().containsKey(SHUFFLE_MERGE_KEY);
    }


    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return wrap(in.findForcedDeletesMerges(segmentInfos, mergeContext));
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                               Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                                               MergeContext mergeContext) throws IOException {
        return wrap(in.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext));
    }

    private MergeSpecification wrap(MergeSpecification mergeSpec) throws IOException {
        if (mergeSpec == null) {
            return null;
        }
        MergeSpecification newMergeSpec = new MergeSpecification();
        for (OneMerge toWrap : mergeSpec.merges) {
            List<SegmentCommitInfo> newInfos = interleaveList(new ArrayList<>(toWrap.segments));
            newMergeSpec.add(new OneMerge(newInfos) {
                @Override
                public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                    return toWrap.wrapForMerge(reader);
                }

                @Override
                public void setMergeInfo(SegmentCommitInfo info) {
                    // record that this segment was merged with interleaved segments
                    Map<String, String> copy = new HashMap<>(info.info.getDiagnostics());
                    copy.put(SHUFFLE_MERGE_KEY, "");
                    info.info.setDiagnostics(copy);
                    super.setMergeInfo(info);
                }
            });
        }

        return newMergeSpec;
    }

    // Return a new list that sort segments of the original one by name (older first)
    // and then interleave them to colocate oldest and most recent segments together.
    private List<SegmentCommitInfo> interleaveList(List<SegmentCommitInfo> infos) throws IOException {
        List<SegmentCommitInfo> newInfos = new ArrayList<>(infos.size());
        Collections.sort(infos, Comparator.comparing(a -> a.info.name));
        int left = 0;
        int right = infos.size() - 1;
        while (left <= right) {
            SegmentCommitInfo leftInfo = infos.get(left);
            if (left == right) {
                newInfos.add(infos.get(left));
            } else {
                SegmentCommitInfo rightInfo = infos.get(right);
                // smaller segment first
                if (leftInfo.sizeInBytes() < rightInfo.sizeInBytes()) {
                    newInfos.add(leftInfo);
                    newInfos.add(rightInfo);
                } else {
                    newInfos.add(rightInfo);
                    newInfos.add(leftInfo);
                }
            }
            left ++;
            right --;
        }
        return newInfos;
    }
}
