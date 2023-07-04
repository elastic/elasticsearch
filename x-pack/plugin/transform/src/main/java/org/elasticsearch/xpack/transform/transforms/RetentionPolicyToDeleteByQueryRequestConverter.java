/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.time.DateFormatter;
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
 * All implementations of `retention_policy` are converted to a {@link DeleteByQueryRequest}, which is then executed by the indexer.
 */
public final class RetentionPolicyToDeleteByQueryRequestConverter {

    private static final String DATE_FORMAT = "strict_date_optional_time";
    private static final DateFormatter DATE_FORMATER = DateFormatter.forPattern(DATE_FORMAT);

    public static class RetentionPolicyException extends ElasticsearchException {
        RetentionPolicyException(String msg, Object... args) {
            super(msg, args);
        }
    }

    private RetentionPolicyToDeleteByQueryRequestConverter() {}

    /**
     * Build a {@link DeleteByQueryRequest} from a `retention policy`. The DBQ runs _after_ all data for a new checkpoint
     * has been processed (composite agg + bulk indexing) and the index got refreshed. After the DBQ - with a final index refresh -
     * the checkpoint is complete.
     *
     * @param retentionPolicyConfig The retention policy configuration
     * @param settingsConfig settings to set certain parameters
     * @param destConfig the destination config
     * @param nextCheckpoint The checkpoint that just finished
     *
     * @return a delete by query request according to the given configurations or null if no delete by query should be executed
     */
    static DeleteByQueryRequest buildDeleteByQueryRequest(
        RetentionPolicyConfig retentionPolicyConfig,
        SettingsConfig settingsConfig,
        DestConfig destConfig,
        TransformCheckpoint nextCheckpoint
    ) {
        if (nextCheckpoint == null || nextCheckpoint.isEmpty()) {
            return null;
        }

        DeleteByQueryRequest request = new DeleteByQueryRequest();

        if (retentionPolicyConfig instanceof TimeRetentionPolicyConfig) {
            request.setQuery(getDeleteQueryFromTimeBasedRetentionPolicy((TimeRetentionPolicyConfig) retentionPolicyConfig, nextCheckpoint));
        } else {
            throw new RetentionPolicyException("unsupported retention policy of type [{}]", retentionPolicyConfig.getWriteableName());
        }

        /* other dbq options not set and why:
         * - timeout: we do not timeout for search, so we don't timeout for dbq
         * - batch size: don't use max page size search, dbq should be simple
         * - refresh: we call refresh separately, after DBQ is executed because refresh should be executed with system permissions
         */
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .setBatchSize(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE)
            // this should not happen, but still go over version conflicts and report later
            .setAbortOnVersionConflict(false)
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
        return QueryBuilders.rangeQuery(config.getField()).lt(DATE_FORMATER.format(cutOffDate)).format(DATE_FORMAT);
    }
}
