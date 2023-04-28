/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.ingest;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * Event ingest is done through a {@link BulkProcessor2}. This class is responsible for instantiating the bulk processor.
 */
public class BulkProcessorFactory {
    private static final Logger logger = LogManager.getLogger(AnalyticsEventEmitter.class);

    private final AnalyticsEventIngestConfig config;

    private final Client client;

    @Inject
    public BulkProcessorFactory(Client client, AnalyticsEventIngestConfig config) {
        this.client = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.config = config;
    }

    public BulkProcessor2 create() {
        return BulkProcessor2.builder(client::bulk, new BulkProcessorListener(), client.threadPool())
            .setMaxNumberOfRetries(config.maxNumberOfRetries())
            .setBulkActions(config.maxNumberOfEventsPerBulk())
            .setFlushInterval(config.flushDelay())
            .setMaxBytesInFlight(config.maxBytesInFlight())
            .build();
    }

    static class BulkProcessorListener implements BulkProcessor2.Listener {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {}

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                List<String> failures = Arrays.stream(response.getItems())
                    .filter(BulkItemResponse::isFailed)
                    .map(r -> r.getId() + " " + r.getFailureMessage())
                    .collect(Collectors.toList());
                logger.error("Bulk write of behavioral analytics events encountered some failures: [{}]", failures);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Exception failure) {
            logger.error(
                "Bulk write of " + request.numberOfActions() + " behavioral analytics events logs failed: " + failure.getMessage(),
                failure
            );
        }
    }
}
