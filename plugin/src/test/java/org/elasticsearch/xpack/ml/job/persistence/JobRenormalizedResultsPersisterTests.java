/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.normalizer.BucketNormalizable;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.BucketInfluencer;

import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobRenormalizedResultsPersisterTests extends ESTestCase {

    public void testUpdateBucket() {
        BucketNormalizable bn = createBucketNormalizable();
        JobRenormalizedResultsPersister persister = createJobRenormalizedResultsPersister();
        persister.updateBucket(bn);

        assertEquals(3, persister.getBulkRequest().numberOfActions());
        assertEquals("foo-index", persister.getBulkRequest().requests().get(0).index());
    }

    public void testExecuteRequestResetsBulkRequest() {
        BucketNormalizable bn = createBucketNormalizable();
        JobRenormalizedResultsPersister persister = createJobRenormalizedResultsPersister();
        persister.updateBucket(bn);
        persister.executeRequest("foo");
        assertEquals(0, persister.getBulkRequest().numberOfActions());
    }

    private JobRenormalizedResultsPersister createJobRenormalizedResultsPersister() {
        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.hasFailures()).thenReturn(false);

        Client client = new MockClientBuilder("cluster").bulk(bulkResponse).build();
        return new JobRenormalizedResultsPersister(Settings.EMPTY, client);
    }

    private BucketNormalizable createBucketNormalizable() {
        Date now = new Date();
        Bucket bucket = new Bucket("foo", now, 1);
        int sequenceNum = 0;
        bucket.addBucketInfluencer(new BucketInfluencer("foo", now, 1, sequenceNum++));
        bucket.addBucketInfluencer(new BucketInfluencer("foo", now, 1, sequenceNum++));
        return new BucketNormalizable(bucket, "foo-index");
    }
}