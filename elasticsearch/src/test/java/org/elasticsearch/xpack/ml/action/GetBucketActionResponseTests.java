/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.GetBucketsAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.results.PartitionScore;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetBucketActionResponseTests extends AbstractStreamableTestCase<GetBucketsAction.Response> {

    @Override
    protected Response createTestInstance() {
        int sequenceNum = 0;

        int listSize = randomInt(10);
        List<Bucket> hits = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String jobId = "foo";
            Bucket bucket = new Bucket(jobId, new Date(randomLong()), randomNonNegativeLong());
            if (randomBoolean()) {
                bucket.setAnomalyScore(randomDouble());
            }
            if (randomBoolean()) {
                int size = randomInt(10);
                List<BucketInfluencer> bucketInfluencers = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    BucketInfluencer bucketInfluencer = new BucketInfluencer("foo", bucket.getTimestamp(), bucket.getBucketSpan(),
                            sequenceNum++);
                    bucketInfluencer.setAnomalyScore(randomDouble());
                    bucketInfluencer.setInfluencerFieldName(randomAsciiOfLengthBetween(1, 20));
                    bucketInfluencer.setInitialAnomalyScore(randomDouble());
                    bucketInfluencer.setProbability(randomDouble());
                    bucketInfluencer.setRawAnomalyScore(randomDouble());
                    bucketInfluencers.add(bucketInfluencer);
                }
                bucket.setBucketInfluencers(bucketInfluencers);
            }
            if (randomBoolean()) {
                bucket.setEventCount(randomNonNegativeLong());
            }
            if (randomBoolean()) {
                bucket.setInitialAnomalyScore(randomDouble());
            }
            if (randomBoolean()) {
                bucket.setInterim(randomBoolean());
            }
            if (randomBoolean()) {
                bucket.setMaxNormalizedProbability(randomDouble());
            }
            if (randomBoolean()) {
                int size = randomInt(10);
                List<PartitionScore> partitionScores = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    partitionScores.add(new PartitionScore(randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                            randomDouble(), randomDouble(), randomDouble()));
                }
                bucket.setPartitionScores(partitionScores);
            }
            if (randomBoolean()) {
                int size = randomInt(10);
                Map<String, Double> perPartitionMaxProbability = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    perPartitionMaxProbability.put(randomAsciiOfLengthBetween(1, 20), randomDouble());
                }
                bucket.setPerPartitionMaxProbability(perPartitionMaxProbability);
            }
            if (randomBoolean()) {
                bucket.setProcessingTimeMs(randomLong());
            }
            if (randomBoolean()) {
                bucket.setRecordCount(randomInt());
            }
            if (randomBoolean()) {
                int size = randomInt(10);
                List<AnomalyRecord> records = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    AnomalyRecord anomalyRecord = new AnomalyRecord(jobId, new Date(randomLong()), randomNonNegativeLong(), sequenceNum++);
                    anomalyRecord.setAnomalyScore(randomDouble());
                    anomalyRecord.setActual(Collections.singletonList(randomDouble()));
                    anomalyRecord.setTypical(Collections.singletonList(randomDouble()));
                    anomalyRecord.setProbability(randomDouble());
                    anomalyRecord.setInterim(randomBoolean());
                    records.add(anomalyRecord);
                }
                bucket.setRecords(records);
            }
            hits.add(bucket);
        }
        QueryPage<Bucket> buckets = new QueryPage<>(hits, listSize, Bucket.RESULTS_FIELD);
        return new Response(buckets);
    }

    @Override
    protected Response createBlankInstance() {
        return new GetBucketsAction.Response();
    }

}
