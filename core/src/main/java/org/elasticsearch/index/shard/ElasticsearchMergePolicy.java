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

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
public final class ElasticsearchMergePolicy extends MergePolicy {
    
    private static ESLogger logger = Loggers.getLogger(ElasticsearchMergePolicy.class);

    private final MergePolicy delegate;

    // True if the next merge request should do segment upgrades:
    private volatile boolean upgradeInProgress;

    // True if the next merge request should only upgrade ancient (an older Lucene major version than current) segments;
    private volatile boolean upgradeOnlyAncientSegments;

    private static final int MAX_CONCURRENT_UPGRADE_MERGES = 5;

    /** @param delegate the merge policy to wrap */
    public ElasticsearchMergePolicy(MergePolicy delegate) {
        this.delegate = delegate;
    }

    /** Return an "upgraded" view of the reader. */
    static CodecReader filter(CodecReader reader) throws IOException {
        // convert 0.90.x _uid payloads to _version docvalues if needed
        reader = VersionFieldUpgrader.wrap(reader);
        // TODO: remove 0.90.x/1.x freqs/prox/payloads from _uid? 
        // the previous code never did this, so some indexes carry around trash.
        return reader;
    }

    static class IndexUpgraderOneMerge extends OneMerge {

        public IndexUpgraderOneMerge(List<SegmentCommitInfo> segments) {
            super(segments);
        }

        @Override
        public List<CodecReader> getMergeReaders() throws IOException {
            final List<CodecReader> newReaders = new ArrayList<>();
            for (CodecReader reader : super.getMergeReaders()) {
                newReaders.add(filter(reader));
            }
            return newReaders;
        }

    }

    static class IndexUpgraderMergeSpecification extends MergeSpecification {

        @Override
        public void add(OneMerge merge) {
            super.add(new IndexUpgraderOneMerge(merge.segments));
        }

        @Override
        public String segString(Directory dir) {
            return "IndexUpgraderMergeSpec[" + super.segString(dir) + "]";
        }

    }

    static MergeSpecification upgradedMergeSpecification(MergeSpecification spec) {
        if (spec == null) {
            return null;
        }
        MergeSpecification upgradedSpec = new IndexUpgraderMergeSpecification();
        for (OneMerge merge : spec.merges) {
            upgradedSpec.add(merge);
        }
        return upgradedSpec;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger,
        SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
        return upgradedMergeSpecification(delegate.findMerges(mergeTrigger, segmentInfos, writer));
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
        int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
        throws IOException {

        if (upgradeInProgress) {
            MergeSpecification spec = new IndexUpgraderMergeSpecification();
            for (SegmentCommitInfo info : segmentInfos) {

                if (shouldUpgrade(info)) {

                    // TODO: Use IndexUpgradeMergePolicy instead.  We should be comparing codecs,
                    // for now we just assume every minor upgrade has a new format.
                    logger.debug("Adding segment " + info.info.name + " to be upgraded");
                    spec.add(new OneMerge(Collections.singletonList(info)));
                }

                // TODO: we could check IndexWriter.getMergingSegments and avoid adding merges that IW will just reject?

                if (spec.merges.size() == MAX_CONCURRENT_UPGRADE_MERGES) {
                    // hit our max upgrades, so return the spec.  we will get a cascaded call to continue.
                    logger.debug("Returning " + spec.merges.size() + " merges for upgrade");
                    return spec;
                }
            }

            // We must have less than our max upgrade merges, so the next return will be our last in upgrading mode.
            if (spec.merges.isEmpty() == false) {
                logger.debug("Returning " + spec.merges.size() + " merges for end of upgrade");
                return spec;
            }

            // Only set this once there are 0 segments needing upgrading, because when we return a
            // spec, IndexWriter may (silently!) reject that merge if some of the segments we asked
            // to be merged were already being (naturally) merged:
            upgradeInProgress = false;

            // fall through, so when we don't have any segments to upgrade, the delegate policy
            // has a chance to decide what to do (e.g. collapse the segments to satisfy maxSegmentCount)
        }

        return upgradedMergeSpecification(delegate.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer));
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
        throws IOException {
        return upgradedMergeSpecification(delegate.findForcedDeletesMerges(segmentInfos, writer));
    }

    @Override
    public boolean useCompoundFile(SegmentInfos segments, SegmentCommitInfo newSegment, IndexWriter writer) throws IOException {
        return delegate.useCompoundFile(segments, newSegment, writer);
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + delegate + ")";
    }

}
