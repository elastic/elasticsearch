/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * Helpers for comparing persisted ML anomaly results in tests while accounting for {@code event.ingested}.
 *
 * <p>{@code event.ingested} is stamped with the wall-clock index time ({@link Instant#now()}) by
 * {@code JobResultsPersister} at write time. Two independent writes of the same logical result therefore carry
 * different {@code event.ingested} values even though every other field is identical (e.g. records re-created after
 * a model-snapshot revert and re-run). {@code equals()} intentionally includes {@code event.ingested} so the field's
 * serialization/renormalization round-trip stays covered, so a direct comparison of two separately-written result sets
 * would spuriously fail.
 *
 * <p>{@code assert*PresentThenClear} asserts the field was populated on persisted documents (write-path coverage) and
 * then clears it so the remaining deterministic fields can be compared with {@code equals()}. {@code clear*} only
 * clears (for an in-memory "expected" object that was never persisted with the field).
 */
public final class AnomalyResultsTestUtils {

    private AnomalyResultsTestUtils() {}

    public static void assertRecordsEventIngestedPresentThenClear(List<AnomalyRecord> records) {
        for (AnomalyRecord record : records) {
            assertNotNull("event.ingested must be set on persisted record", record.getEventIngested());
            record.setEventIngested(null);
        }
    }

    public static void clearRecordsEventIngested(List<AnomalyRecord> records) {
        records.forEach(r -> r.setEventIngested(null));
    }

    public static void assertInfluencersEventIngestedPresentThenClear(List<Influencer> influencers) {
        for (Influencer influencer : influencers) {
            assertNotNull("event.ingested must be set on persisted influencer", influencer.getEventIngested());
            influencer.setEventIngested(null);
        }
    }

    public static void clearInfluencersEventIngested(List<Influencer> influencers) {
        influencers.forEach(i -> i.setEventIngested(null));
    }

    public static void assertBucketEventIngestedPresentThenClear(Bucket bucket) {
        assertNotNull("event.ingested must be set on persisted bucket", bucket.getEventIngested());
        bucket.setEventIngested(null);
        bucket.getBucketInfluencers().forEach(bi -> bi.setEventIngested(null));
    }

    public static void clearBucketEventIngested(Bucket bucket) {
        bucket.setEventIngested(null);
        bucket.getBucketInfluencers().forEach(bi -> bi.setEventIngested(null));
    }

    public static void assertBucketInfluencersEventIngestedPresent(List<BucketInfluencer> bucketInfluencers) {
        for (BucketInfluencer bucketInfluencer : bucketInfluencers) {
            assertNotNull("event.ingested must be set on persisted bucket influencer", bucketInfluencer.getEventIngested());
        }
    }
}
