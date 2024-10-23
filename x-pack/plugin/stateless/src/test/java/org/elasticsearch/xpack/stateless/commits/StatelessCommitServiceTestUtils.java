/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.index.shard.ShardId;

import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility class to access package private methods of the StatelessCommitService in testing outside of that package.
 */
public class StatelessCommitServiceTestUtils {

    private StatelessCommitServiceTestUtils() {}

    public static void updateCommitUseTrackingForInactiveShards(StatelessCommitService statelessCommitService, Supplier<Long> time) {
        statelessCommitService.updateCommitUseTrackingForInactiveShards(time);
    }

    public static Set<String> getAllSearchNodesRetainingCommitsForShard(StatelessCommitService statelessCommitService, ShardId shardId) {
        return statelessCommitService.getAllSearchNodesRetainingCommitsForShard(shardId);
    }
}
