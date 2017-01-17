/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.process.normalizer.BucketNormalizable;
import org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * Interface for classes that update {@linkplain Bucket Buckets}
 * for a particular job with new normalized anomaly scores and
 * unusual scores.
 * <p>
 * Renormalized results must already have an ID.
 */
public class JobRenormalizedResultsPersister extends AbstractComponent {

    private final Client client;
    private BulkRequest bulkRequest;

    public JobRenormalizedResultsPersister(Settings settings, Client client) {
        super(settings);
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
        try {
            XContentBuilder content = toXContentBuilder(resultDoc);
            bulkRequest.add(new IndexRequest(index, Result.TYPE.getPreferredName(), id).source(content));
        } catch (IOException e) {
            logger.error("Error serialising result", e);
        }
    }

    private XContentBuilder toXContentBuilder(ToXContent obj) throws IOException {
        XContentBuilder builder = jsonBuilder();
        obj.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }

    /**
     * Execute the bulk action
     *
     * @param jobId The job Id
     */
    public void executeRequest(String jobId) {
        if (bulkRequest.numberOfActions() == 0) {
            return;
        }
        logger.trace("[{}] ES API CALL: bulk request with {} actions", jobId, bulkRequest.numberOfActions());

        BulkResponse addRecordsResponse = client.bulk(bulkRequest).actionGet();
        if (addRecordsResponse.hasFailures()) {
            logger.error("[{}] Bulk index of results has errors: {}", jobId, addRecordsResponse.buildFailureMessage());
        }
    }

    BulkRequest getBulkRequest() {
        return bulkRequest;
    }
}

