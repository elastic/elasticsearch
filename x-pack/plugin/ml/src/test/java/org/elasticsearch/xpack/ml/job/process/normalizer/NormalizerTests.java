/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NormalizerTests extends ESTestCase {
    private static final String JOB_ID = "foo";
    private static final String INDEX_NAME = "foo-index";
    private static final String QUANTILES_STATE = "someState";
    private static final int BUCKET_SPAN = 600;
    private static final double INITIAL_SCORE = 3.0;
    private static final double FACTOR = 2.0;

    private Bucket generateBucket(Date timestamp) {
        return new Bucket(JOB_ID, timestamp, BUCKET_SPAN);
    }

    private BucketInfluencer createTimeBucketInfluencer(Date timestamp, double probability, double anomalyScore) {
        BucketInfluencer influencer = new BucketInfluencer(JOB_ID, timestamp, BUCKET_SPAN);
        influencer.setInfluencerFieldName(BucketInfluencer.BUCKET_TIME);
        influencer.setProbability(probability);
        influencer.setInitialAnomalyScore(anomalyScore);
        influencer.setAnomalyScore(anomalyScore);
        return influencer;
    }

    public void testNormalize() throws IOException, InterruptedException {
        ExecutorService threadpool = Executors.newScheduledThreadPool(1);
        try {
            NormalizerProcessFactory processFactory = mock(NormalizerProcessFactory.class);
            when(processFactory.createNormalizerProcess(eq(JOB_ID), eq(QUANTILES_STATE), eq(BUCKET_SPAN), any())).thenReturn(
                new MultiplyingNormalizerProcess(FACTOR)
            );
            Normalizer normalizer = new Normalizer(JOB_ID, processFactory, threadpool);

            Bucket bucket = generateBucket(new Date(0));
            bucket.setAnomalyScore(0.0);
            bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.07, INITIAL_SCORE));

            List<Normalizable> asNormalizables = Arrays.asList(new BucketNormalizable(bucket, INDEX_NAME));
            normalizer.normalize(BUCKET_SPAN, asNormalizables, QUANTILES_STATE);

            assertEquals(1, asNormalizables.size());
            assertEquals(FACTOR * INITIAL_SCORE, asNormalizables.get(0).getNormalizedScore(), 0.0001);
        } finally {
            threadpool.shutdown();
        }
        assertTrue(threadpool.awaitTermination(1, TimeUnit.SECONDS));
    }
}
