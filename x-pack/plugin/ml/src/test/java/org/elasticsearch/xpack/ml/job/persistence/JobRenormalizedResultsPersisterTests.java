/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.normalizer.BucketNormalizable;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;

import java.util.Date;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
        persister.executeRequest();
        assertEquals(0, persister.getBulkRequest().numberOfActions());
    }

    public void testBulkRequestExecutesWhenReachMaxDocs() {
        BulkResponse bulkResponse = mock(BulkResponse.class);
        Client client = new MockClientBuilder("cluster").bulk(bulkResponse).build();
        JobRenormalizedResultsPersister persister = new JobRenormalizedResultsPersister("foo", client);

        ModelPlot modelPlot = new ModelPlot("foo", new Date(), 123456, 0);
        for (int i=0; i<=JobRenormalizedResultsPersister.BULK_LIMIT; i++) {
            persister.updateResult("bar", "index-foo", modelPlot);
        }

        verify(client, times(1)).bulk(any());
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    private JobRenormalizedResultsPersister createJobRenormalizedResultsPersister() {
        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.hasFailures()).thenReturn(false);

        Client client = new MockClientBuilder("cluster").bulk(bulkResponse).build();
        return new JobRenormalizedResultsPersister("foo", client);
    }

    private BucketNormalizable createBucketNormalizable() {
        Date now = new Date();
        Bucket bucket = new Bucket("foo", now, 1);
        bucket.addBucketInfluencer(new BucketInfluencer("foo", now, 1));
        bucket.addBucketInfluencer(new BucketInfluencer("foo", now, 1));
        return new BucketNormalizable(bucket, "foo-index");
    }
}
