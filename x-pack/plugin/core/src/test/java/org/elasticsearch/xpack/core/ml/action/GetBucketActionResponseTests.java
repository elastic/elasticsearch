/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction.Response;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class GetBucketActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
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
                    BucketInfluencer bucketInfluencer = new BucketInfluencer("foo", bucket.getTimestamp(), bucket.getBucketSpan());
                    bucketInfluencer.setAnomalyScore(randomDouble());
                    bucketInfluencer.setInfluencerFieldName(randomAlphaOfLengthBetween(1, 20));
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
                bucket.setProcessingTimeMs(randomLong());
            }
            if (randomBoolean()) {
                int size = randomInt(10);
                List<AnomalyRecord> records = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    AnomalyRecord anomalyRecord = new AnomalyRecord(jobId, new Date(randomLong()), randomNonNegativeLong());
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
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
