/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility class to access package private methods of the StatelessCommitService in testing outside of that package.
 */
public class StatelessCommitServiceTestUtils {

    private static final Logger logger = LogManager.getLogger(StatelessCommitServiceTestUtils.class);

    private StatelessCommitServiceTestUtils() {}

    public static void updateCommitUseTrackingForInactiveShards(StatelessCommitService statelessCommitService, Supplier<Long> time) {
        statelessCommitService.updateCommitUseTrackingForInactiveShards(time);
    }

    public static Set<String> getAllSearchNodesRetainingCommitsForShard(StatelessCommitService statelessCommitService, ShardId shardId) {
        return statelessCommitService.getAllSearchNodesRetainingCommitsForShard(shardId);
    }

    public static StatelessCommitCleaner getStatelessCommitCleaner(StatelessCommitService statelessCommitService) {
        return statelessCommitService.getCommitCleaner();
    }

    public static void logBlobReferences(StatelessCommitService statelessCommitService, ShardId shardId, Level logLevel) {
        final var commitState = statelessCommitService.getSafe(shardId);
        logger.log(logLevel, "blob references of shard [{}]: {}", shardId, commitState.getPrimaryTermAndGenToBlobReferences());
    }
}
