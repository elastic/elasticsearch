/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.base.Splitter;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.AuthenticationService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 *
 */
public class IndexAuditTrailBulkProcessor extends AbstractLifecycleComponent<IndexAuditTrailBulkProcessor> {

    public static final int DEFAULT_BULK_SIZE = 1000;
    public static final int MAX_BULK_SIZE = 10000;
    public static final String INDEX_NAME_PREFIX = ".shield-audit-log";
    public static final String DOC_TYPE = "event";

    public static final TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(1);
    public static final IndexNameResolver.Rollover DEFAULT_ROLLOVER = IndexNameResolver.Rollover.DAILY;

    private static BulkProcessor bulkProcessor;

    private final Provider<Client> clientProvider;
    private final IndexAuditUserHolder auditUser;
    private final AuthenticationService authenticationService;
    private final IndexNameResolver resolver;
    private final IndexNameResolver.Rollover rollover;
    private final Environment environment;

    private Client client;
    private boolean indexToRemoteCluster;

    @Inject
    public IndexAuditTrailBulkProcessor(Settings settings, Environment environment, AuthenticationService authenticationService,
                                        IndexAuditUserHolder auditUser, Provider<Client> clientProvider) {
        super(settings);
        this.authenticationService = authenticationService;
        this.auditUser = auditUser;
        this.clientProvider = clientProvider;
        this.environment = environment;

        IndexNameResolver.Rollover rollover;
        try {
            rollover = IndexNameResolver.Rollover.valueOf(
                    settings.get("shield.audit.index.rollover", DEFAULT_ROLLOVER.name()).toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            logger.warn("invalid value for setting [shield.audit.index.rollover]; falling back to default [{}]",
                    DEFAULT_ROLLOVER.name());
            rollover = DEFAULT_ROLLOVER;
        }
        this.rollover = rollover;
        this.resolver = new IndexNameResolver();
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        initializeClient();
        initializeBulkProcessor();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        try {
            if (bulkProcessor != null) {
                bulkProcessor.close();
            }
        } finally {
            if (indexToRemoteCluster) {
                if (client != null) {
                    client.close();
                }
            }
        }
    }

    public void submit(IndexAuditTrail.Message message) {
        assert lifecycle.started();
        IndexRequest indexRequest = client.prepareIndex()
                .setIndex(resolver.resolve(INDEX_NAME_PREFIX, message.timestamp, rollover))
                .setType(DOC_TYPE).setSource(message.builder).request();
        authenticationService.attachUserHeaderIfMissing(indexRequest, auditUser.user());
        bulkProcessor.add(indexRequest);
    }

    private void initializeClient() {

        Settings clientSettings = settings.getByPrefix("shield.audit.index.client.");

        if (clientSettings.names().size() == 0) {
            // in the absence of client settings for remote indexing, fall back to the client that was passed in.
            this.client = clientProvider.get();
            indexToRemoteCluster = false;
        } else {
            String[] hosts = clientSettings.getAsArray("hosts");
            if (hosts.length == 0) {
                throw new ElasticsearchException("missing required setting " +
                        "[shield.audit.index.client.hosts] for remote audit log indexing");
            }

            if (clientSettings.get("cluster.name", "").isEmpty()) {
                throw new ElasticsearchException("missing required setting " +
                        "[shield.audit.index.client.cluster.name] for remote audit log indexing");
            }

            List<Tuple<String, Integer>> hostPortPairs = new ArrayList<>();

            for (String host : hosts) {
                List<String> hostPort = Splitter.on(":").splitToList(host.trim());
                if (hostPort.size() != 1 && hostPort.size() != 2) {
                    logger.warn("invalid host:port specified: [{}] for setting [shield.audit.index.client.hosts]", host);
                }
                hostPortPairs.add(new Tuple<>(hostPort.get(0), hostPort.size() == 2 ? Integer.valueOf(hostPort.get(1)) : 9300));
            }

            if (hostPortPairs.size() == 0) {
                throw new ElasticsearchException("no valid host:port pairs specified for setting [shield.audit.index.client.hosts]");
            }

            final TransportClient transportClient = TransportClient.builder()
                    .settings(Settings.builder().put(clientSettings).put("path.home", environment.homeFile()).build()).build();
            for (Tuple<String, Integer> pair : hostPortPairs) {
                transportClient.addTransportAddress(new InetSocketTransportAddress(pair.v1(), pair.v2()));
            }

            this.client = transportClient;
            indexToRemoteCluster = true;

            logger.info("forwarding audit events to remote cluster [{}] using hosts [{}]",
                    clientSettings.get("cluster.name", ""), hostPortPairs.toString());
        }
    }

    private void initializeBulkProcessor() {

        int bulkSize = Math.min(settings.getAsInt("shield.audit.index.bulk_size", DEFAULT_BULK_SIZE), MAX_BULK_SIZE);
        bulkSize = (bulkSize < 1) ? DEFAULT_BULK_SIZE : bulkSize;

        TimeValue interval = settings.getAsTime("shield.audit.index.flush_interval", DEFAULT_FLUSH_INTERVAL);
        interval = (interval.millis() < 1) ? DEFAULT_FLUSH_INTERVAL : interval;

        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                authenticationService.attachUserHeaderIfMissing(request, auditUser.user());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    logger.info("failed to bulk index audit events: [{}]", response.buildFailureMessage());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("failed to bulk index audit events: [{}]", failure, failure.getMessage());
            }
        }).setBulkActions(bulkSize)
                .setFlushInterval(interval)
                .setConcurrentRequests(1)
                .build();
    }
}
