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
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class StatelessCommitCleaner {
    private final Logger logger = LogManager.getLogger(StatelessCommitCleaner.class);

    private final StatelessClusterConsistencyService consistencyService;

    public StatelessCommitCleaner(StatelessClusterConsistencyService consistencyService) {
        this.consistencyService = consistencyService;
    }

    void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
        logger.debug("--> deleting compound commit {}", staleCompoundCommit);
        // No-op
    }

    public record StaleCompoundCommit(ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration) {}
}
