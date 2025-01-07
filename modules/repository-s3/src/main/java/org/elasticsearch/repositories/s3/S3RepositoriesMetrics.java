/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;

public record S3RepositoriesMetrics(
    RepositoriesMetrics common,
    LongCounter retryStartedCounter,
    LongCounter retryCompletedCounter,
    LongHistogram retryHistogram,
    LongHistogram retryDeletesHistogram
) {

    public static final S3RepositoriesMetrics NOOP = new S3RepositoriesMetrics(RepositoriesMetrics.NOOP);

    public static final String METRIC_RETRY_EVENT_TOTAL = "es.repositories.s3.input_stream.retry.event.total";
    public static final String METRIC_RETRY_SUCCESS_TOTAL = "es.repositories.s3.input_stream.retry.success.total";
    public static final String METRIC_RETRY_ATTEMPTS_HISTOGRAM = "es.repositories.s3.input_stream.retry.attempts.histogram";
    public static final String METRIC_DELETE_RETRIES_HISTOGRAM = "es.repositories.s3.delete.retry.attempts.histogram";

    public S3RepositoriesMetrics(RepositoriesMetrics common) {
        this(
            common,
            common.meterRegistry().registerLongCounter(METRIC_RETRY_EVENT_TOTAL, "s3 input stream retry event count", "unit"),
            common.meterRegistry().registerLongCounter(METRIC_RETRY_SUCCESS_TOTAL, "s3 input stream retry success count", "unit"),
            common.meterRegistry()
                .registerLongHistogram(METRIC_RETRY_ATTEMPTS_HISTOGRAM, "s3 input stream retry attempts histogram", "unit"),
            common.meterRegistry().registerLongHistogram(METRIC_DELETE_RETRIES_HISTOGRAM, "s3 delete retry attempts histogram", "unit")
        );
    }
}
