/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.Request;
import com.amazonaws.util.AWSRequestMetrics;
import com.amazonaws.util.TimingInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.util.Maps;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * This class emit aws s3 metrics as logs until we have a proper apm integration
 */
public class S3RequestRetryStats {
    public static final String MESSAGE_FIELD = "message";

    private static final Logger logger = LogManager.getLogger(S3RequestRetryStats.class);

    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong exceptions = new AtomicLong();
    private final AtomicLong throttles = new AtomicLong();
    private final AtomicLongArray exceptionsHistogram;
    private final AtomicLongArray throttlesHistogram;

    public S3RequestRetryStats(int maxRetries) {
        this.exceptionsHistogram = new AtomicLongArray(maxRetries + 1);
        this.throttlesHistogram = new AtomicLongArray(maxRetries + 1);
    }

    public void addRequest(Request<?> request) {
        if (request == null) {
            return;
        }
        var info = request.getAWSRequestMetrics().getTimingInfo();
        long requests = getCounter(info, AWSRequestMetrics.Field.RequestCount);
        long exceptions = getCounter(info, AWSRequestMetrics.Field.Exception);
        long throttles = getCounter(info, AWSRequestMetrics.Field.ThrottleException);

        this.requests.addAndGet(requests);
        this.exceptions.addAndGet(exceptions);
        this.throttles.addAndGet(throttles);
        if (exceptions >= 0 && exceptions < this.exceptionsHistogram.length()) {
            this.exceptionsHistogram.incrementAndGet((int) exceptions);
        }
        if (throttles >= 0 && throttles < this.throttlesHistogram.length()) {
            this.throttlesHistogram.incrementAndGet((int) throttles);
        }
    }

    private static long getCounter(TimingInfo info, AWSRequestMetrics.Field field) {
        var counter = info.getCounter(field.name());
        return counter != null ? counter.longValue() : 0L;
    }

    public void emitMetrics() {
        if (logger.isDebugEnabled()) {
            var metrics = Maps.<String, Object>newMapWithExpectedSize(4);
            metrics.put(MESSAGE_FIELD, "S3 Request Retry Stats");
            metrics.put("elasticsearch.metrics.s3.requests", requests.get());
            metrics.put("elasticsearch.metrics.s3.exceptions", exceptions.get());
            metrics.put("elasticsearch.metrics.s3.throttles", throttles.get());
            for (int i = 0; i < exceptionsHistogram.length(); i++) {
                long exceptions = exceptionsHistogram.get(i);
                if (exceptions != 0) {
                    metrics.put("elasticsearch.metrics.s3.exceptions_histogram_" + i, exceptions);
                }
            }
            for (int i = 0; i < throttlesHistogram.length(); i++) {
                long throttles = throttlesHistogram.get(i);
                if (throttles != 0) {
                    metrics.put("elasticsearch.metrics.s3.throttles_histogram_" + i, throttles);
                }
            }
            logger.debug(new ESLogMessage().withFields(metrics));
        }
    }
}
