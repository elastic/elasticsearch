/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.process.normalizer.BucketNormalizable;
import org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
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
    private BulkRequest bulkRequest;

    public JobRenormalizedResultsPersister(String jobId, Client client) {
        this.jobId = jobId;
        this.client = client;
        bulkRequest = new BulkRequest();
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
            bulkRequest.add(new IndexRequest(index).id(id).source(content));
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] Error serialising result", jobId), e);
        }
        if (bulkRequest.numberOfActions() >= BULK_LIMIT) {
            executeRequest();
        }
    }

    private XContentBuilder toXContentBuilder(ToXContent obj) throws IOException {
        XContentBuilder builder = jsonBuilder();
        obj.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }

    /**
     * Execute the bulk action
     */
    public void executeRequest() {
        if (bulkRequest.numberOfActions() == 0) {
            return;
        }
        logger.trace("[{}] ES API CALL: bulk request with {} actions", jobId, bulkRequest.numberOfActions());

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            BulkResponse addRecordsResponse = client.bulk(bulkRequest).actionGet();
            if (addRecordsResponse.hasFailures()) {
                logger.error("[{}] Bulk index of results has errors: {}", jobId, addRecordsResponse.buildFailureMessage());
            }
        }

        bulkRequest = new BulkRequest();
    }

    BulkRequest getBulkRequest() {
        return bulkRequest;
    }
}

