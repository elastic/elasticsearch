/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;

import java.util.Objects;

/**
 * Builds the appropriate {@link DelayedDataDetector} implementation, with the appropriate settings, given the parameters.
 */
public class DelayedDataDetectorFactory {

    // There are eight 15min buckets in a two hour span, so matching that number as the fallback for very long buckets
    private static final int DEFAULT_NUMBER_OF_BUCKETS_TO_SPAN = 8;
    private static final long DEFAULT_CHECK_WINDOW_MS = 7_200_000L; // 2 hours in Milliseconds

    /**
     * This will build the appropriate detector given the parameters.
     *
     * If {@link DatafeedConfig#getDelayedDataCheckConfig()} is not `isEnabled()`, then a {@link NullDelayedDataDetector} is returned, which
     * does not do any checks, and only supplies an empty collection.
     *
     * @param job The {@link Job} object for the given `datafeedConfig`
     * @param datafeedConfig The {@link DatafeedConfig} for which to create the {@link DelayedDataDetector}
     * @param client The {@link Client} capable of taking action against the ES Cluster.
     * @param xContentRegistry The current NamedXContentRegistry with which to parse the query
     * @return A new {@link DelayedDataDetector}
     */
    public static DelayedDataDetector buildDetector(Job job,
                                                    DatafeedConfig datafeedConfig,
                                                    Client client,
                                                    NamedXContentRegistry xContentRegistry) {
        if (datafeedConfig.getDelayedDataCheckConfig().isEnabled()) {
            long window = validateAndCalculateWindowLength(job.getAnalysisConfig().getBucketSpan(),
                datafeedConfig.getDelayedDataCheckConfig().getCheckWindow());
            long bucketSpan = job.getAnalysisConfig().getBucketSpan() == null ? 0 : job.getAnalysisConfig().getBucketSpan().millis();
            return new DatafeedDelayedDataDetector(bucketSpan,
                window,
                job.getId(),
                job.getDataDescription().getTimeField(),
                datafeedConfig.getParsedQuery(xContentRegistry),
                datafeedConfig.getIndices().toArray(new String[0]),
                datafeedConfig.getIndicesOptions(),
                client);
        } else {
            return new NullDelayedDataDetector();
        }
    }

    private static long validateAndCalculateWindowLength(TimeValue bucketSpan, TimeValue currentWindow) {
        if (bucketSpan == null) {
            return 0;
        }
        if (currentWindow == null) { // we should provide a good default as the user did not specify a window
            return Math.max(DEFAULT_CHECK_WINDOW_MS, DEFAULT_NUMBER_OF_BUCKETS_TO_SPAN * bucketSpan.millis());
        }
        if (currentWindow.compareTo(bucketSpan) < 0) {
            throw new IllegalArgumentException(
                Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL, currentWindow.getStringRep(),
                    bucketSpan.getStringRep()));
        } else if (currentWindow.millis() > bucketSpan.millis() * DelayedDataCheckConfig.MAX_NUMBER_SPANABLE_BUCKETS) {
            throw new IllegalArgumentException(
                Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_SPANS_TOO_MANY_BUCKETS, currentWindow.getStringRep(),
                    bucketSpan.getStringRep()));
        }
        return currentWindow.millis();
    }

    public static class BucketWithMissingData {

        private final long missingDocumentCount;
        private final Bucket bucket;

        public static BucketWithMissingData fromMissingAndBucket(long missingDocumentCount, Bucket bucket) {
            return new BucketWithMissingData(missingDocumentCount, bucket);
        }

        private BucketWithMissingData(long missingDocumentCount, Bucket bucket) {
            this.missingDocumentCount = missingDocumentCount;
            this.bucket = bucket;
        }

        public long getTimeStamp() {
            return bucket.getEpoch();
        }

        public Bucket getBucket() {
            return bucket;
        }

        public long getMissingDocumentCount() {
            return missingDocumentCount;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            BucketWithMissingData that = (BucketWithMissingData) other;

            return Objects.equals(that.bucket, bucket) && Objects.equals(that.missingDocumentCount, missingDocumentCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucket, missingDocumentCount);
        }
    }

}
