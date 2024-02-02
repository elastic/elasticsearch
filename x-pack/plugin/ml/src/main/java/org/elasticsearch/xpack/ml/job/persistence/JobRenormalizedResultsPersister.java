/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.process.normalizer.BucketNormalizable;
import org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Interface for classes that update {@linkplain Bucket Buckets}
 * for a particular job with new normalized anomaly scores and
 * unusual scores.
 * <p>
 * Renormalized results must already have an ID.
 * <p>
 * This class is NOT thread safe.
 */
public class JobRenormalizedResultsPersister {

    private static final Logger logger = LogManager.getLogger(JobRenormalizedResultsPersister.class);

    /**
     * Execute bulk requests when they reach this size
     */
    static final int BULK_LIMIT = 10000;

    private final String jobId;
    private final Client client;
    private BulkRequestBuilder bulkRequestBuilder;

    public JobRenormalizedResultsPersister(String jobId, Client client) {
        this.jobId = jobId;
        this.client = client;
        bulkRequestBuilder = client.prepareBulk();
    }

    public void updateBucket(BucketNormalizable normalizable) {
        updateResult(normalizable.getId(), normalizable.getOriginatingIndex(), normalizable.getBucket());
        updateBucketInfluencersStandalone(normalizable.getOriginatingIndex(), normalizable.getBucket().getBucketInfluencers());
    }

    private void updateBucketInfluencersStandalone(String indexName, List<BucketInfluencer> bucketInfluencers) {
        if (bucketInfluencers != null && bucketInfluencers.isEmpty() == false) {
            for (BucketInfluencer bucketInfluencer : bucketInfluencers) {
                updateResult(bucketInfluencer.getId(), indexName, bucketInfluencer);
            }
        }
    }

    public void updateResults(List<Normalizable> normalizables) {
        for (Normalizable normalizable : normalizables) {
            updateResult(normalizable.getId(), normalizable.getOriginatingIndex(), normalizable);
        }
    }

    public void updateResult(String id, String index, ToXContent resultDoc) {
        try (XContentBuilder content = toXContentBuilder(resultDoc)) {
            bulkRequestBuilder.add(client.prepareIndex(index).setId(id).setSource(content));
        } catch (IOException e) {
            logger.error(() -> "[" + jobId + "] Error serialising result", e);
        }
        if (bulkRequestBuilder.numberOfActions() >= BULK_LIMIT) {
            executeRequest();
        }
    }

    private static XContentBuilder toXContentBuilder(ToXContent obj) throws IOException {
        XContentBuilder builder = jsonBuilder();
        obj.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }

    /**
     * Execute the bulk action
     */
    public void executeRequest() {
        if (bulkRequestBuilder.numberOfActions() == 0) {
            return;
        }
        logger.trace("[{}] ES API CALL: bulk request with {} actions", jobId, bulkRequestBuilder.numberOfActions());

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            BulkRequest bulkRequest = bulkRequestBuilder.request();
            try {
                BulkResponse addRecordsResponse = client.bulk(bulkRequest).actionGet();
                if (addRecordsResponse.hasFailures()) {
                    logger.error("[{}] Bulk index of results has errors: {}", jobId, addRecordsResponse.buildFailureMessage());
                }
            } finally {
                bulkRequest.decRef();
            }
        }

        bulkRequestBuilder = client.prepareBulk();
    }

    BulkRequestBuilder getBulkRequest() {
        return bulkRequestBuilder;
    }
}
