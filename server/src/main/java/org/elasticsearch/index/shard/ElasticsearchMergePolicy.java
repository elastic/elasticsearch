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

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.Version;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A {@link MergePolicy} that upgrades segments and can upgrade merges.
 * <p>
 * It can be useful to use the background merging process to upgrade segments,
 * for example when we perform internal changes that imply different index
 * options or when a user modifies his mapping in non-breaking ways: we could
 * imagine using this merge policy to be able to add doc values to fields after
 * the fact or on the opposite to remove them.
 * <p>
 * For now, this {@link MergePolicy} takes care of moving versions that used to
 * be stored as payloads to numeric doc values.
 */
public final class ElasticsearchMergePolicy extends FilterMergePolicy {

    private static final Logger logger = LogManager.getLogger(ElasticsearchMergePolicy.class);

    // True if the next merge request should do segment upgrades:
    private volatile boolean upgradeInProgress;

    // True if the next merge request should only upgrade ancient (an older Lucene major version than current) segments;
    private volatile boolean upgradeOnlyAncientSegments;

    private static final int MAX_CONCURRENT_UPGRADE_MERGES = 5;

    /** @param delegate the merge policy to wrap */
    public ElasticsearchMergePolicy(MergePolicy delegate) {
        super(delegate);
    }

    /** return the wrapped merge policy */
    public MergePolicy getDelegate() {
        return in;
    }

    private boolean shouldUpgrade(SegmentCommitInfo info) {
        org.apache.lucene.util.Version old = info.info.getVersion();
        org.apache.lucene.util.Version cur = Version.CURRENT.luceneVersion;

        // Something seriously wrong if this trips:
        assert old.major <= cur.major;

        if (cur.major > old.major) {
            // Always upgrade segment if Lucene's major version is too old
            return true;
        }
        if (upgradeOnlyAncientSegments == false && cur.minor > old.minor) {
            // If it's only a minor version difference, and we are not upgrading only ancient segments,
            // also upgrade:
            return true;
        }
        // Version matches, or segment is not ancient and we are only upgrading ancient segments:
        return false;
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
        int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, MergeContext mergeContext)
        throws IOException {

        if (upgradeInProgress) {
            MergeSpecification spec = new MergeSpecification();
            for (SegmentCommitInfo info : segmentInfos) {

                if (shouldUpgrade(info)) {

                    // TODO: Use IndexUpgradeMergePolicy instead.  We should be comparing codecs,
                    // for now we just assume every minor upgrade has a new format.
                    logger.debug("Adding segment {} to be upgraded", info.info.name);
                    spec.add(new OneMerge(Collections.singletonList(info)));
                }

                // TODO: we could check IndexWriter.getMergingSegments and avoid adding merges that IW will just reject?

                if (spec.merges.size() == MAX_CONCURRENT_UPGRADE_MERGES) {
                    // hit our max upgrades, so return the spec.  we will get a cascaded call to continue.
                    logger.debug("Returning {} merges for upgrade", spec.merges.size());
                    return spec;
                }
            }

            // We must have less than our max upgrade merges, so the next return will be our last in upgrading mode.
            if (spec.merges.isEmpty() == false) {
                logger.debug("Returning {} merges for end of upgrade", spec.merges.size());
                return spec;
            }

            // Only set this once there are 0 segments needing upgrading, because when we return a
            // spec, IndexWriter may (silently!) reject that merge if some of the segments we asked
            // to be merged were already being (naturally) merged:
            upgradeInProgress = false;

            // fall through, so when we don't have any segments to upgrade, the delegate policy
            // has a chance to decide what to do (e.g. collapse the segments to satisfy maxSegmentCount)
        }

        return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
    }

    /**
     * When <code>upgrade</code> is true, running a force merge will upgrade any segments written
     * with older versions. This will apply to the next call to
     * {@link IndexWriter#forceMerge} that is handled by this {@link MergePolicy}, as well as
     * cascading calls made by {@link IndexWriter}.
     */
    public void setUpgradeInProgress(boolean upgrade, boolean onlyAncientSegments) {
        this.upgradeInProgress = upgrade;
        this.upgradeOnlyAncientSegments = onlyAncientSegments;
    }
}
