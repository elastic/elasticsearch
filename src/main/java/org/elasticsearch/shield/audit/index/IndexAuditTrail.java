/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import com.google.common.base.Splitter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.rest.RemoteHostHeader;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.shield.audit.AuditUtil.indices;
import static org.elasticsearch.shield.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.*;

/**
 * Audit trail implementation that writes events into an index.
 */
public class IndexAuditTrail extends AbstractLifecycleComponent<IndexAuditTrail> implements AuditTrail {

    public static final int DEFAULT_BULK_SIZE = 1000;
    public static final int MAX_BULK_SIZE = 10000;
    public static final TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(1);
    public static final IndexNameResolver.Rollover DEFAULT_ROLLOVER = IndexNameResolver.Rollover.DAILY;
    public static final String NAME = "index";
    public static final String INDEX_NAME_PREFIX = ".shield-audit-log";
    public static final String DOC_TYPE = "event";

    static final String[] DEFAULT_EVENT_INCLUDES = new String[] {
            ACCESS_DENIED.toString(),
            ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(),
            AUTHENTICATION_FAILED.toString(),
            CONNECTION_DENIED.toString(),
            CONNECTION_GRANTED.toString(),
            TAMPERED_REQUEST.toString()
    };

    private final String nodeName;
    private final IndexAuditUserHolder auditUser;
    private final Provider<Client> clientProvider;
    private final AuthenticationService authenticationService;
    private final IndexNameResolver resolver;
    private final Environment environment;

    private BulkProcessor bulkProcessor;
    private Client client;
    private boolean indexToRemoteCluster;
    private IndexNameResolver.Rollover rollover;
    private String nodeHostName;
    private String nodeHostAddress;

    @Override
    public String name() {
        return NAME;
    }

    private EnumSet<IndexAuditLevel> events;

    @Inject
    public IndexAuditTrail(Settings settings, IndexAuditUserHolder indexingAuditUser,
                           Environment environment, AuthenticationService authenticationService,
                           Provider<Client> clientProvider) {
        super(settings);
        this.auditUser = indexingAuditUser;
        this.authenticationService = authenticationService;
        this.clientProvider = clientProvider;
        this.environment = environment;
        this.resolver = new IndexNameResolver();
        this.nodeName = settings.get("name");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        try {
            rollover = IndexNameResolver.Rollover.valueOf(
                    settings.get("shield.audit.index.rollover", DEFAULT_ROLLOVER.name()).toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            logger.warn("invalid value for setting [shield.audit.index.rollover]; falling back to default [{}]",
                    DEFAULT_ROLLOVER.name());
            rollover = DEFAULT_ROLLOVER;
        }

        String hostname = "n/a";
        String hostaddr = "n/a";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            hostaddr = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("unable to resolve local host name", e);
        }
        this.nodeHostName = hostname;
        this.nodeHostAddress = hostaddr;

        String[] includedEvents = settings.getAsArray("shield.audit.index.events.include", DEFAULT_EVENT_INCLUDES);
        String[] excludedEvents = settings.getAsArray("shield.audit.index.events.exclude");
        events = parse(includedEvents, excludedEvents);

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

    @Override
    public void anonymousAccessDenied(String action, TransportMessage<?> message) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            try {
                submit(message("anonymous_access_denied", action, null, null, indices(message), message));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [anonymous_access_denied]", e);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            try {
                submit(message("anonymous_access_denied", null, null, null, null, request));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [anonymous_access_denied]", e);
            }
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage<?> message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            try {
                submit(message("authentication_failed", action, null, null, indices(message), message));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [authentication_failed]", e);
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            try {
                submit(message("authentication_failed", null, null, null, null, request));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [authentication_failed]", e);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage<?> message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    submit(message("authentication_failed", action, token.principal(), null, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    submit(message("authentication_failed", null, token.principal(), null, null, request));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage<?> message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    submit(message("authentication_failed", action, token.principal(), realm, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (!principalIsAuditor(token.principal())) {
                try {
                    submit(message("authentication_failed", null, token.principal(), realm, null, request));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage<?> message) {
        if (!principalIsAuditor(user.principal())) {
            // special treatment for internal system actions - only log if explicitly told to
            if (user.isSystem() && Privilege.SYSTEM.predicate().apply(action)) {
                if (events.contains(SYSTEM_ACCESS_GRANTED)) {
                    try {
                        submit(message("access_granted", action, user.principal(), null, indices(message), message));
                    } catch (Exception e) {
                        logger.warn("failed to index audit event: [access_granted]", e);
                    }
                }
            } else if (events.contains(ACCESS_GRANTED)) {
                try {
                    submit(message("access_granted", action, user.principal(), null, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [access_granted]", e);
                }
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage<?> message) {
        if (events.contains(ACCESS_DENIED)) {
            if (!principalIsAuditor(user.principal())) {
                try {
                    submit(message("access_denied", action, user.principal(), null, indices(message), message));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [access_denied]", e);
                }
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportRequest request) {
        if (events.contains(TAMPERED_REQUEST)) {
            if (!principalIsAuditor(user.principal())) {
                try {
                    submit(message("tampered_request", action, user.principal(), null, indices(request), request));
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [tampered_request]", e);
                }
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED)) {
            try {
                submit(message("ip_filter", "connection_granted", inetAddress, profile, rule));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_granted]", e);
            }
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED)) {
            try {
                submit(message("ip_filter", "connection_denied", inetAddress, profile, rule));
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_denied]", e);
            }
        }
    }

    private boolean principalIsAuditor(String principal) {
        return (principal.equals(auditUser.user().principal()));
    }

    private Message message(String type, @Nullable String action, @Nullable String principal,
                            @Nullable String realm, @Nullable String indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }
        if (principal != null) {
            msg.builder.field(Field.PRINCIPAL, principal);
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (indices != null) {
            msg.builder.field(Field.INDICES, indices);
        }
        if (logger.isDebugEnabled()) {
            msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());
        }

        return msg.end();
    }

    private Message message(String type, @Nullable String action, @Nullable String principal,
                            @Nullable String realm, @Nullable String indices, RestRequest request) throws Exception {

        Message msg = new Message().start();
        common("rest", type, msg.builder);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }
        if (principal != null) {
            msg.builder.field(Field.PRINCIPAL, principal);
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (indices != null) {
            msg.builder.field(Field.INDICES, indices);
        }
        if (logger.isDebugEnabled()) {
            msg.builder.field(Field.REQUEST_BODY, restRequestContent(request));
        }

        msg.builder.field(Field.ORIGIN_ADDRESS, request.getRemoteAddress());
        msg.builder.field(Field.URI, request.uri());

        return msg.end();
    }

    private Message message(String layer, String type, InetAddress originAddress, String profile,
                            ShieldIpFilterRule rule) throws IOException {

        Message msg = new Message().start();
        common(layer, type, msg.builder);

        msg.builder.field(Field.ORIGIN_ADDRESS, originAddress.getHostAddress());
        msg.builder.field(Field.TRANSPORT_PROFILE, profile);
        msg.builder.field(Field.RULE, rule);

        return msg.end();
    }

    private XContentBuilder common(String layer, String type, XContentBuilder builder) throws IOException {
        builder.field(Field.NODE_NAME, nodeName);
        builder.field(Field.NODE_HOST_NAME, nodeHostName);
        builder.field(Field.NODE_HOST_ADDRESS, nodeHostAddress);
        builder.field(Field.LAYER, layer);
        builder.field(Field.TYPE, type);
        return builder;
    }

    private static XContentBuilder originAttributes(TransportMessage message, XContentBuilder builder) throws IOException {

        // first checking if the message originated in a rest call
        InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(message);
        if (restAddress != null) {
            builder.field(Field.ORIGIN_TYPE, "rest");
            builder.field(Field.ORIGIN_ADDRESS, restAddress);
            return builder;
        }

        // we'll see if was originated in a remote node
        TransportAddress address = message.remoteAddress();
        if (address != null) {
            builder.field(Field.ORIGIN_TYPE, "transport");
            if (address instanceof InetSocketTransportAddress) {
                builder.field(Field.ORIGIN_ADDRESS, ((InetSocketTransportAddress) address).address());
            } else {
                builder.field(Field.ORIGIN_ADDRESS, address);
            }
            return builder;
        }

        // the call was originated locally on this node
        builder.field(Field.ORIGIN_TYPE, "local_node");
        builder.field(Field.ORIGIN_ADDRESS, NetworkUtils.getLocalHostAddress("_local"));
        return builder;
    }

    void submit(IndexAuditTrail.Message message) {
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

    static class Message {

        final long timestamp;
        final XContentBuilder builder;

        Message() throws IOException {
            this.timestamp = System.currentTimeMillis();
            this.builder = XContentFactory.jsonBuilder();
        }

        Message start() throws IOException {
            builder.startObject();
            builder.field(Field.TIMESTAMP, timestamp);
            return this;
        }

        Message end() throws IOException {
            builder.endObject();
            return this;
        }
    }

    interface Field {
        XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        XContentBuilderString NODE_NAME = new XContentBuilderString("node_name");
        XContentBuilderString NODE_HOST_NAME = new XContentBuilderString("node_host_name");
        XContentBuilderString NODE_HOST_ADDRESS = new XContentBuilderString("node_host_address");
        XContentBuilderString LAYER = new XContentBuilderString("layer");
        XContentBuilderString TYPE = new XContentBuilderString("type");
        XContentBuilderString ORIGIN_ADDRESS = new XContentBuilderString("origin_address");
        XContentBuilderString ORIGIN_TYPE = new XContentBuilderString("origin_type");
        XContentBuilderString PRINCIPAL = new XContentBuilderString("principal");
        XContentBuilderString ACTION = new XContentBuilderString("action");
        XContentBuilderString INDICES = new XContentBuilderString("indices");
        XContentBuilderString REQUEST = new XContentBuilderString("request");
        XContentBuilderString REQUEST_BODY = new XContentBuilderString("request_body");
        XContentBuilderString URI = new XContentBuilderString("uri");
        XContentBuilderString REALM = new XContentBuilderString("realm");
        XContentBuilderString TRANSPORT_PROFILE = new XContentBuilderString("transport_profile");
        XContentBuilderString RULE = new XContentBuilderString("rule");
    }
}
