/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;


import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class ResultsPersisterService {
    private static final Logger LOGGER = LogManager.getLogger(ResultsPersisterService.class);

    public static final Setting<Integer> PERSIST_RESULTS_MAX_RETRIES = Setting.intSetting(
        "xpack.ml.persist_results_max_retries",
        15,
        0,
        50,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope);
    private static final int MAX_RETRY_SLEEP_MILLIS = 900_000; // 15 min
    private static final int MIN_RETRY_SLEEP_MILLIS = 50;
    // Having an exponent higher than this causes integer overflow
    private static final int MAX_RETRY_EXPONENT = 29;

    private final Random random = Randomness.get();
    private final Client client;
    private volatile int maxFailureRetries;

    public ResultsPersisterService(Client client, ClusterService clusterService, Settings settings) {
        this.client = client;
        this.maxFailureRetries = PERSIST_RESULTS_MAX_RETRIES.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(PERSIST_RESULTS_MAX_RETRIES, this::setMaxFailureRetries);
    }

    void setMaxFailureRetries(int value) {
        this.maxFailureRetries = value;
    }

    public BulkResponse indexWithRetry(String jobId,
                                       String indexName,
                                       ToXContent object,
                                       ToXContent.Params params,
                                       WriteRequest.RefreshPolicy refreshPolicy,
                                       String id,
                                       Supplier<Boolean> shouldRetry) throws IOException {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
        try (XContentBuilder content = object.toXContent(XContentFactory.jsonBuilder(), params)) {
            bulkRequest.add(new IndexRequest(indexName).id(id).source(content));
        }
        return bulkIndexWithRetry(bulkRequest, jobId, shouldRetry);
    }

    public BulkResponse bulkIndexWithRetry(BulkRequest bulkRequest, String jobId, Supplier<Boolean> shouldRetry) {
        int currentMin = MIN_RETRY_SLEEP_MILLIS;
        int currentMax = MIN_RETRY_SLEEP_MILLIS;
        int currentAttempt = 0;
        while(currentAttempt <= maxFailureRetries) {
            BulkResponse bulkResponse = bulkIndex(bulkRequest);
            if (bulkResponse.hasFailures() == false) {
                return bulkResponse;
            }
            if (shouldRetry.get() == false) {
                throw new ElasticsearchException("[{}] failed to index all results. {}", jobId, bulkResponse.buildFailureMessage());
            }
            if (currentAttempt > maxFailureRetries) {
                LOGGER.warn("[{}] failed to index after [{}] attempts.", jobId, currentAttempt);
                throw new ElasticsearchException("[{}] failed to index all results after [{}] attempts. {}",
                    jobId,
                    currentAttempt,
                    bulkResponse.buildFailureMessage());
            }
            currentAttempt++;
            // Since we exponentially increase, we don't want force randomness to have an excessively long sleep
            if (currentMax < MAX_RETRY_SLEEP_MILLIS) {
                currentMin = currentMax;
            }
            double backOff = ((1 << Math.min(currentAttempt, MAX_RETRY_EXPONENT)) - 1) / 2.0;
            int max = (int)(backOff * 100);
            currentMax = Math.min(max, MAX_RETRY_SLEEP_MILLIS);
            // Its good to have a random window along the exponentially increasing curve
            // so that not all bulk requests rest for the same amount of time
            int randSleep = currentMin + random.nextInt(1 + (currentMax - currentMin));
            // We should only retry the docs that failed.
            bulkRequest = buildNewRequestFromFailures(bulkRequest, bulkResponse);
            try {
                Thread.sleep(randSleep);
            } catch (InterruptedException interruptedException) {
                LOGGER.warn(
                    new ParameterizedMessage("[{}] failed to index after [{}] attempts due to interruption",
                        jobId,
                        currentAttempt),
                    interruptedException);
                Thread.currentThread().interrupt();
            }
        }
        throw new ElasticsearchException("[{}] failed to index all results after [{}] attempts. {}",
            jobId,
            currentAttempt);
    }

    private BulkResponse bulkIndex(BulkRequest bulkRequest) {
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            return client.bulk(bulkRequest).actionGet();
        }
    }

    private BulkRequest buildNewRequestFromFailures(BulkRequest bulkRequest, BulkResponse bulkResponse) {
        // If we failed, lets set the bulkRequest to be a collection of the failed requests
        BulkRequest bulkRequestOfFailures = new BulkRequest();
        Set<String> failedDocIds = new HashSet<>();
        bulkResponse.forEach(itemResponse -> {
            if (itemResponse.isFailed()) {
                failedDocIds.add(itemResponse.getId());
            }
        });
        bulkRequest.requests().forEach(docWriteRequest -> {
            if (failedDocIds.contains(docWriteRequest.id())) {
                bulkRequestOfFailures.add(docWriteRequest);
            }
        });
        return bulkRequestOfFailures;
    }

    public static class BulkIndexException extends Exception {

        public BulkIndexException(String msg) {
            super(msg);
        }

        public BulkIndexException(BulkResponse bulkResponse) {
            this(bulkResponse.buildFailureMessage());
        }

    }

}
