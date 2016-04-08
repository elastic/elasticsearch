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

package org.elasticsearch.index.merge;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;

import java.util.List;

/**
 * Represents a single on going merge within an index.
 */
public class OnGoingMerge {

    private final String id;
    private final List<SegmentCommitInfo> mergedSegments;

    public OnGoingMerge(MergePolicy.OneMerge merge) {
        this.id = Integer.toString(System.identityHashCode(merge));
        this.mergedSegments = merge.segments;
    }

    /**
     * A unique id for the merge.
     */
    public String getId() {
        return id;
    }

    /**
     * The list of segments that are being merged.
     */
    public List<SegmentCommitInfo> getMergedSegments() {
        return mergedSegments;
    }
}
