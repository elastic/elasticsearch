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
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ResultsPersisterService {
    private static final Logger LOGGER = LogManager.getLogger(ResultsPersisterService.class);

    public static final Setting<Integer> PERSIST_RESULTS_MAX_RETRIES = Setting.intSetting(
        "xpack.ml.persist_results_max_retries",
        20,
        0,
        50,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope);
    private static final int MAX_RETRY_SLEEP_MILLIS = (int)Duration.ofMinutes(15).toMillis();
    private static final int MIN_RETRY_SLEEP_MILLIS = 50;
    // Having an exponent higher than this causes integer overflow
    private static final int MAX_RETRY_EXPONENT = 24;

    private final CheckedConsumer<Integer, InterruptedException> sleeper;
    private final OriginSettingClient client;
    private volatile int maxFailureRetries;

    public ResultsPersisterService(OriginSettingClient client, ClusterService clusterService, Settings settings) {
        this(Thread::sleep, client, clusterService, settings);
    }

    // Visible for testing
    ResultsPersisterService(CheckedConsumer<Integer, InterruptedException> sleeper,
                            OriginSettingClient client,
                            ClusterService clusterService,
                            Settings settings) {
        this.sleeper = sleeper;
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
                                       Supplier<Boolean> shouldRetry,
                                       Consumer<String> msgHandler) throws IOException {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
        try (XContentBuilder content = object.toXContent(XContentFactory.jsonBuilder(), params)) {
            bulkRequest.add(new IndexRequest(indexName).id(id).source(content));
        }
        return bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, msgHandler);
    }

    public BulkResponse bulkIndexWithRetry(BulkRequest bulkRequest,
                                           String jobId,
                                           Supplier<Boolean> shouldRetry,
                                           Consumer<String> msgHandler) {
        return bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, msgHandler,
            providedBulkRequest -> client.bulk(providedBulkRequest).actionGet());
    }

    public BulkResponse bulkIndexWithHeadersWithRetry(Map<String, String> headers,
                                                      BulkRequest bulkRequest,
                                                      String jobId,
                                                      Supplier<Boolean> shouldRetry,
                                                      Consumer<String> msgHandler) {
        return bulkIndexWithRetry(bulkRequest, jobId, shouldRetry, msgHandler,
            providedBulkRequest -> ClientHelper.executeWithHeaders(
                headers, ClientHelper.ML_ORIGIN, client, () -> client.execute(BulkAction.INSTANCE, bulkRequest).actionGet()));
    }

    private BulkResponse bulkIndexWithRetry(BulkRequest bulkRequest,
                                            String jobId,
                                            Supplier<Boolean> shouldRetry,
                                            Consumer<String> msgHandler,
                                            Function<BulkRequest, BulkResponse> actionExecutor) {
        RetryContext retryContext = new RetryContext(jobId, shouldRetry, msgHandler);
        while (true) {
            BulkResponse bulkResponse = actionExecutor.apply(bulkRequest);
            if (bulkResponse.hasFailures() == false) {
                return bulkResponse;
            }

            retryContext.nextIteration("index", bulkResponse.buildFailureMessage());

            // We should only retry the docs that failed.
            bulkRequest = buildNewRequestFromFailures(bulkRequest, bulkResponse);
        }
    }

    public SearchResponse searchWithRetry(SearchRequest searchRequest,
                                          String jobId,
                                          Supplier<Boolean> shouldRetry,
                                          Consumer<String> msgHandler) {
        RetryContext retryContext = new RetryContext(jobId, shouldRetry, msgHandler);
        while (true) {
            String failureMessage;
            try {
                SearchResponse searchResponse = client.search(searchRequest).actionGet();
                if (RestStatus.OK.equals(searchResponse.status())) {
                    return searchResponse;
                }
                failureMessage = searchResponse.status().toString();
            } catch (ElasticsearchException e) {
                LOGGER.warn("[" + jobId + "] Exception while executing search action", e);
                failureMessage = e.getDetailedMessage();
            }

            retryContext.nextIteration("search", failureMessage);
        }
    }

    /**
     * {@link RetryContext} object handles logic that is executed between consecutive retries of an action.
     *
     * Note that it does not execute the action itself.
     */
    private class RetryContext {

        final String jobId;
        final Supplier<Boolean> shouldRetry;
        final Consumer<String> msgHandler;
        final Random random = Randomness.get();

        int currentAttempt = 0;
        int currentMin = MIN_RETRY_SLEEP_MILLIS;
        int currentMax = MIN_RETRY_SLEEP_MILLIS;

        RetryContext(String jobId, Supplier<Boolean> shouldRetry, Consumer<String> msgHandler) {
            this.jobId = jobId;
            this.shouldRetry = shouldRetry;
            this.msgHandler = msgHandler;
        }

        void nextIteration(String actionName, String failureMessage) {
            currentAttempt++;

            // If the outside conditions have changed and retries are no longer needed, do not retry.
            if (shouldRetry.get() == false) {
                String msg = new ParameterizedMessage(
                    "[{}] should not retry {} after [{}] attempts. {}", jobId, actionName, currentAttempt, failureMessage)
                    .getFormattedMessage();
                LOGGER.info(msg);
                throw new ElasticsearchException(msg);
            }

            // If the configured maximum number of retries has been reached, do not retry.
            if (currentAttempt > maxFailureRetries) {
                String msg = new ParameterizedMessage(
                    "[{}] failed to {} after [{}] attempts. {}", jobId, actionName, currentAttempt, failureMessage).getFormattedMessage();
                LOGGER.warn(msg);
                throw new ElasticsearchException(msg);
            }

            // Since we exponentially increase, we don't want force randomness to have an excessively long sleep
            if (currentMax < MAX_RETRY_SLEEP_MILLIS) {
                currentMin = currentMax;
            }
            // Exponential backoff calculation taken from: https://en.wikipedia.org/wiki/Exponential_backoff
            int uncappedBackoff = ((1 << Math.min(currentAttempt, MAX_RETRY_EXPONENT)) - 1) * (50);
            currentMax = Math.min(uncappedBackoff, MAX_RETRY_SLEEP_MILLIS);
            // Its good to have a random window along the exponentially increasing curve
            // so that not all bulk requests rest for the same amount of time
            int randBound = 1 + (currentMax - currentMin);
            int randSleep = currentMin + random.nextInt(randBound);
            {
                String msg = new ParameterizedMessage(
                    "failed to {} after [{}] attempts. Will attempt again in [{}].",
                    actionName,
                    currentAttempt,
                    TimeValue.timeValueMillis(randSleep).getStringRep())
                    .getFormattedMessage();
                LOGGER.warn(() -> new ParameterizedMessage("[{}] {}", jobId, msg));
                msgHandler.accept(msg);
            }
            try {
                sleeper.accept(randSleep);
            } catch (InterruptedException interruptedException) {
                LOGGER.warn(
                    new ParameterizedMessage("[{}] failed to {} after [{}] attempts due to interruption",
                        jobId,
                        actionName,
                        currentAttempt),
                    interruptedException);
                Thread.currentThread().interrupt();
            }
        }
    }

    private BulkRequest buildNewRequestFromFailures(BulkRequest bulkRequest, BulkResponse bulkResponse) {
        // If we failed, lets set the bulkRequest to be a collection of the failed requests
        BulkRequest bulkRequestOfFailures = new BulkRequest();
        Set<String> failedDocIds = Arrays.stream(bulkResponse.getItems())
            .filter(BulkItemResponse::isFailed)
            .map(BulkItemResponse::getId)
            .collect(Collectors.toSet());
        bulkRequest.requests().forEach(docWriteRequest -> {
            if (failedDocIds.contains(docWriteRequest.id())) {
                bulkRequestOfFailures.add(docWriteRequest);
            }
        });
        return bulkRequestOfFailures;
    }

}
