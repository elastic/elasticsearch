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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public class SeqNoAwareDeletionPolicy extends IndexDeletionPolicy {

    private final LongSupplier globalCheckpointSupplier;
    private final ESLogger logger;

    public SeqNoAwareDeletionPolicy(LongSupplier globalCheckpointSupplier, ESLogger logger) {
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.logger = logger;
    }


    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        // nocommit - for now no clean up - we clean up at the end of recovery, when we commit
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final long globalCheckpoint = globalCheckpointSupplier.getAsLong();
        List<IndexCommit> toDelete = new ArrayList<>();
        IndexCommit highestBellowCheckpoint;
        SeqNoStats highestBellowCheckpointStats = null;
        int size = commits.size();
        for (int i = 0; i < size - 1; i++) {
            final IndexCommit commit = commits.get(i);
            final SeqNoStats commitStats = Store.loadSeqNoStatsFromCommit(commit);
            if (commitStats.getMaxSeqNo() > globalCheckpoint) {
                // have to stay
            } else if (highestBellowCheckpointStats == null || highestBellowCheckpointStats.getMaxSeqNo() < commitStats.getMaxSeqNo()) {
                highestBellowCheckpoint = commit;
                highestBellowCheckpointStats = commitStats;
                if (highestBellowCheckpoint != null) {
                    logger.trace("will deleted commit [{}] with seqNoStats [{}], global checkpoint [{}]",
                        highestBellowCheckpoint.getSegmentsFileName(), highestBellowCheckpointStats, globalCheckpoint);
                    toDelete.add(highestBellowCheckpoint);
                }
            } else {
                logger.trace("will deleted commit [{}] with seqNoStats [{}], global checkpoint [{}]", commit.getSegmentsFileName(),
                    commitStats, globalCheckpoint);
                toDelete.add(commit);
            }
        }

        toDelete.stream().forEach(IndexCommit::delete);
    }

}
