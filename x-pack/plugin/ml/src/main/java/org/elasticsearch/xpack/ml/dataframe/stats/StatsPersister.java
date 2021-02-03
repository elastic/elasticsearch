/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;

public class StatsPersister {

    private static final Logger LOGGER = LogManager.getLogger(StatsPersister.class);

    private final String jobId;
    private final ResultsPersisterService resultsPersisterService;
    private final DataFrameAnalyticsAuditor auditor;

    public StatsPersister(String jobId, ResultsPersisterService resultsPersisterService, DataFrameAnalyticsAuditor auditor) {
        this.jobId = Objects.requireNonNull(jobId);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.auditor = Objects.requireNonNull(auditor);
    }

    public void persistWithRetry(ToXContentObject result, Function<String, String> docIdSupplier) {
        try {
            resultsPersisterService.indexWithRetry(jobId,
                MlStatsIndex.writeAlias(),
                result,
                new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")),
                WriteRequest.RefreshPolicy.NONE,
                docIdSupplier.apply(jobId),
                true,
                () -> true,
                retryMessage ->
                    LOGGER.debug("[{}] failed to persist result with id [{}]; {}", jobId, docIdSupplier.apply(jobId), retryMessage)
            );
        } catch (IOException ioe) {
            LOGGER.error(() -> new ParameterizedMessage("[{}] Failed serializing stats result", jobId), ioe);
        } catch (Exception e) {
            LOGGER.error(() -> new ParameterizedMessage("[{}] Failed indexing stats result", jobId), e);
            auditor.error(jobId, "Failed indexing stats result with id [" + docIdSupplier.apply(jobId) + "]; " + e.getMessage());
        }
    }
}
