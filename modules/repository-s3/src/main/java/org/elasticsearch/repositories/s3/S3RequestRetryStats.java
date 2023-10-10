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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * This class emit aws s3 metrics as logs until we have a proper apm integration
 */
public class S3RequestRetryStats {

    private static final Logger logger = LogManager.getLogger(S3RequestRetryStats.class);

    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong throttles = new AtomicLong();
    private final AtomicLongArray retriesHistogram;
    private final AtomicLongArray throttlesHistogram;

    public S3RequestRetryStats(int maxRetries) {
        this.retriesHistogram = new AtomicLongArray(maxRetries);
        this.throttlesHistogram = new AtomicLongArray(maxRetries);
    }

    public void addRequest(Request<?> request) {
        var info = request.getAWSRequestMetrics().getTimingInfo();
        long requests = getCounter(info, AWSRequestMetrics.Field.RequestCount);
        long retries = getCounter(info, AWSRequestMetrics.Field.Exception);
        long throttles = getCounter(info, AWSRequestMetrics.Field.ThrottleException);

        this.requests.addAndGet(requests);
        this.retries.addAndGet(retries);
        this.throttles.addAndGet(throttles);
        if (retries >= 0 && retries < this.retriesHistogram.length()) {
            this.retriesHistogram.incrementAndGet((int) retries);
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
            emitMetric("elasticsearch.s3.requests", requests.get());
            emitMetric("elasticsearch.s3.retries", retries.get());
            emitMetric("elasticsearch.s3.throttles", throttles.get());
            for (int i = 0; i < retriesHistogram.length(); i++) {
                emitMetric("elasticsearch.s3.retries." + i, retriesHistogram.get(i));
            }
            for (int i = 0; i < throttlesHistogram.length(); i++) {
                emitMetric("elasticsearch.s3.throttles." + i, throttlesHistogram.get(i));
            }
        }
    }

    private void emitMetric(String name, long value) {
        logger.debug(new ESLogMessage().withFields(Map.of(name, value)));
    }
}
