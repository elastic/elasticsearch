/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;

import java.time.Instant;

/**
 * Implementation of `retention_policy` configuration parameter.
 *
 * All implementations of `rentention_policy` are converted to a {@link DeleteByQueryRequest}, which is than executed by the indexer.
 */
public final class RetentionPolicyToDeleteByQueryRequestConverter {

    public static class RetentionPolicyException extends ElasticsearchException {
        RetentionPolicyException(String msg, Object... args) {
            super(msg, args);
        }
    }

    private RetentionPolicyToDeleteByQueryRequestConverter() {}

    /**
     * Build a {@link DeleteByQueryRequest} from a retention policy. The DBQ should run _after_ a new checkpoint has finished.
     * The given checkpoint should be the one that just finished indexing, however the DBQ executes before this checkpoint
     * gets exposed.
     *
     * @param retentionPolicyConfig The retention policy configuration
     * @param settingsConfig settings to set certain parameters
     * @param destConfig the destination config
     * @param checkpoint The checkpoint that just finished
     *
     * @return a delete by query request according to the given configurations or null if no delete by query should be executed
     */
    static DeleteByQueryRequest buildDeleteByQueryRequest(
        RetentionPolicyConfig retentionPolicyConfig,
        SettingsConfig settingsConfig,
        DestConfig destConfig,
        TransformCheckpoint checkpoint
    ) {
        if (checkpoint == null || checkpoint.isEmpty()) {
            return null;
        }

        DeleteByQueryRequest request = new DeleteByQueryRequest();

        if (retentionPolicyConfig instanceof TimeRetentionPolicyConfig) {
            request.setQuery(getDeleteQueryFromTimeBasedRetentionPolicy((TimeRetentionPolicyConfig) retentionPolicyConfig, checkpoint));
        } else {
            throw new RetentionPolicyException("unsupported retention policy of type [{}]", retentionPolicyConfig.getWriteableName());
        }

        /* other dbq options not set and why:
         * - timeout: we do not timeout for search, so we don't timeout for dbq
         * - batch size: don't use max page size search, dbq should be simple
         */
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .setBatchSize(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE)
            // this should not happen, but still go over version conflicts and report later
            .setAbortOnVersionConflict(false)
            // refresh the index, so docs are really gone
            .setRefresh(true)
            // use transforms retry mechanics instead
            .setMaxRetries(0)
            .indices(destConfig.getIndex());

        // use the same throttling as for search
        if (settingsConfig.getDocsPerSecond() != null) {
            request.setRequestsPerSecond(settingsConfig.getDocsPerSecond());
        }

        return request;
    }

    private static QueryBuilder getDeleteQueryFromTimeBasedRetentionPolicy(
        TimeRetentionPolicyConfig config,
        TransformCheckpoint checkpoint
    ) {
        Instant cutOffDate = Instant.ofEpochMilli(checkpoint.getTimestamp()).minusMillis(config.getMaxAge().getMillis());
        return QueryBuilders.rangeQuery(config.getField()).lt(cutOffDate.toEpochMilli()).format("epoch_millis");
    }
}
