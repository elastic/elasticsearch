/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import java.time.Duration;

/**
 * Factory methods for a sensible default for the datafeed frequency
 */
public final class DefaultFrequency {
    private static final int SECONDS_IN_MINUTE = 60;
    private static final int TWO_MINS_SECONDS = 2 * SECONDS_IN_MINUTE;
    private static final int TWENTY_MINS_SECONDS = 20 * SECONDS_IN_MINUTE;
    private static final int HALF_DAY_SECONDS = 12 * 60 * SECONDS_IN_MINUTE;
    private static final Duration TEN_MINUTES = Duration.ofMinutes(10);
    private static final Duration ONE_HOUR = Duration.ofHours(1);

    private DefaultFrequency() {
        // Do nothing
    }

    /**
     * Creates a sensible default frequency for a given bucket span.
     * <p>
     * The default depends on the bucket span:
     * <ul>
     * <li> &lt;= 2 mins -&gt; 1 min</li>
     * <li> &lt;= 20 mins -&gt; bucket span / 2</li>
     * <li> &lt;= 12 hours -&gt; 10 mins</li>
     * <li> &gt; 12 hours -&gt; 1 hour</li>
     * </ul>
     *
     * @param bucketSpanSeconds the bucket span in seconds
     * @return the default frequency
     */
    public static Duration ofBucketSpan(long bucketSpanSeconds) {
        if (bucketSpanSeconds <= 0) {
            throw new IllegalArgumentException("Bucket span has to be > 0");
        }

        if (bucketSpanSeconds <= TWO_MINS_SECONDS) {
            return Duration.ofSeconds(SECONDS_IN_MINUTE);
        }
        if (bucketSpanSeconds <= TWENTY_MINS_SECONDS) {
            return Duration.ofSeconds(bucketSpanSeconds / 2);
        }
        if (bucketSpanSeconds <= HALF_DAY_SECONDS) {
            return TEN_MINUTES;
        }
        return ONE_HOUR;
    }
}
