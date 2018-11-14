/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;

import java.util.Objects;

public class DelayedDataDetectorFactory {

    private static final int FALLBACK_NUMBER_OF_BUCKETS_TO_SPAN = 7;

    public static DelayedDataDetector buildDetector(Job job, DatafeedConfig datafeedConfig, Client client) {
        if (datafeedConfig.getDelayedDataCheckConfig().isEnabled()) {
            long window = validateAndCalculateWindowLength(job.getAnalysisConfig().getBucketSpan(), datafeedConfig.getDelayedDataCheckConfig().getCheckWindow());
            long bucketSpan = job.getAnalysisConfig().getBucketSpan() == null ? 0 : job.getAnalysisConfig().getBucketSpan().millis();
            return new DatafeedDelayedDataDetector(bucketSpan,
                window,
                job.getId(),
                job.getDataDescription().getTimeField(),
                datafeedConfig.getQuery(),
                datafeedConfig.getIndices().toArray(new String[0]),
                client);
        } else {
            return new NullDelayedDataDetector();
        }
    }

    private static long validateAndCalculateWindowLength(TimeValue bucketSpan, TimeValue currentWindow) {
        if (bucketSpan == null || currentWindow == null) {
            return 0;
        }
        if (currentWindow.compareTo(bucketSpan) < 0) {
            // If it is the default value, we assume the user did not set it and the bucket span is very large
            if (currentWindow.equals(DelayedDataCheckConfig.DEFAULT_DELAYED_DATA_WINDOW)) {
                //TODO What if bucket_span > 24h?, weird but possible case...
                return bucketSpan.millis() * FALLBACK_NUMBER_OF_BUCKETS_TO_SPAN;
            } else {
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL, currentWindow.getStringRep(),
                        bucketSpan.getStringRep()));
            }
        } else if (currentWindow.millis() > bucketSpan.millis() * 10_000) {
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
