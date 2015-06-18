/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.rest.RemoteHostHeader;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.shield.audit.AuditUtil.indices;
import static org.elasticsearch.shield.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.*;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.resolve;

/**
 * Audit trail implementation that writes events into an index.
 */
public class IndexAuditTrail extends AbstractComponent implements AuditTrail {

    public static final int DEFAULT_BULK_SIZE = 1000;
    public static final int MAX_BULK_SIZE = 10000;
    public static final TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(1);
    public static final IndexNameResolver.Rollover DEFAULT_ROLLOVER = IndexNameResolver.Rollover.DAILY;
    public static final String NAME = "index";
    public static final String INDEX_NAME_PREFIX = ".shield_audit_log";
    public static final String DOC_TYPE = "event";
    public static final String ROLLOVER_SETTING = "shield.audit.index.rollover";

    static final String INDEX_TEMPLATE_NAME = "shield_audit_log";
    static final String[] DEFAULT_EVENT_INCLUDES = new String[] {
            ACCESS_DENIED.toString(),
            ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(),
            AUTHENTICATION_FAILED.toString(),
            CONNECTION_DENIED.toString(),
            CONNECTION_GRANTED.toString(),
            TAMPERED_REQUEST.toString()
    };

    private static final ImmutableSet<String> forbiddenIndexSettings = ImmutableSet.of("index.mapper.dynamic");

    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
    private final String nodeName;
    private final IndexAuditUserHolder auditUser;
    private final Provider<Client> clientProvider;
    private final AuthenticationService authenticationService;
    private final Environment environment;

    private BulkProcessor bulkProcessor;
    private Client client;
    private boolean indexToRemoteCluster;
    private IndexNameResolver.Rollover rollover;
    private String nodeHostName;
    private String nodeHostAddress;
    private ConcurrentLinkedQueue<Message> eventQueue = new ConcurrentLinkedQueue<>();
    private EnumSet<IndexAuditLevel> events;

    @Override
    public String name() {
        return NAME;
    }

    @Inject
    public IndexAuditTrail(Settings settings, IndexAuditUserHolder indexingAuditUser,
                           Environment environment, AuthenticationService authenticationService,
                           Provider<Client> clientProvider) {
        super(settings);
        this.auditUser = indexingAuditUser;
        this.authenticationService = authenticationService;
        this.clientProvider = clientProvider;
        this.environment = environment;
        this.nodeName = settings.get("name");

        // we have to initialize this here since we use rollover in determining if we can start...
        try {
            rollover = IndexNameResolver.Rollover.valueOf(
                    settings.get(ROLLOVER_SETTING, DEFAULT_ROLLOVER.name()).toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            logger.warn("invalid value for setting [shield.audit.index.rollover]; falling back to default [{}]",
                    DEFAULT_ROLLOVER.name());
            rollover = DEFAULT_ROLLOVER;
        }

        // we have to initialize the events here since we can receive events before starting...
        String[] includedEvents = settings.getAsArray("shield.audit.index.events.include", DEFAULT_EVENT_INCLUDES);
        String[] excludedEvents = settings.getAsArray("shield.audit.index.events.exclude");
        try {
            events = parse(includedEvents, excludedEvents);
        } catch (ShieldException e) {
            logger.warn("invalid event type specified, using default for audit index output. include events [{}], exclude events [{}]", e, includedEvents, excludedEvents);
            events = parse(DEFAULT_EVENT_INCLUDES, Strings.EMPTY_ARRAY);
        }
    }

    public State state() {
        return state.get();
    }

    /**
     * This method determines if this service can be started based on the state in the {@link ClusterChangedEvent} and
     * if the node is the master or not. In order for the service to start, the following must be true:
     *
     * <ol>
     *     <li>The cluster must not have a {@link GatewayService#STATE_NOT_RECOVERED_BLOCK}; in other words the gateway
     *         must have recovered from disk already.</li>
     *     <li>The current node must be the master OR the <code>shield_audit_log</code> index template must exist</li>
     *     <li>The current audit index must not exist or have all primary shards active. The current audit index name
     *         is determined by the rollover settings and current time</li>
     * </ol>
     *
     * @param event the {@link ClusterChangedEvent} containing the up to date cluster state
     * @param master flag indicating if the current node is the master
     * @return true if all requirements are met and the service can be started
     */
    public boolean canStart(ClusterChangedEvent event, boolean master) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .shield-audit-
            // but they may not have been restored from the cluster state on disk
            logger.debug("index audit trail waiting until gateway has recovered from disk");
            return false;
        }

        final ClusterState clusterState = event.state();
        if (!master && clusterState.metaData().templates().get(INDEX_TEMPLATE_NAME) == null) {
            logger.debug("shield audit index template [{}] does not exist, so service cannot start", INDEX_TEMPLATE_NAME);
            return false;
        }

        String index = resolve(INDEX_NAME_PREFIX, DateTime.now(DateTimeZone.UTC), rollover);
        IndexMetaData metaData = clusterState.metaData().index(index);
        if (metaData == null) {
            logger.debug("shield audit index [{}] does not exist, so service can start", index);
            return true;
        }

        if (clusterState.routingTable().index(index).allPrimaryShardsActive()) {
            logger.debug("shield audit index [{}] all primary shards started, so service can start", index);
            return true;
        }
        logger.debug("shield audit index [{}] does not have all primary shards started, so service cannot start", index);
        return false;
    }

    /**
     * Starts the service. The state is moved to {@link org.elasticsearch.shield.audit.index.IndexAuditTrail.State#STARTING}
     * at the beginning of the method. The service's components are initialized and if the current node is the master, the index
     * template will be stored. The state is moved {@link org.elasticsearch.shield.audit.index.IndexAuditTrail.State#STARTED}
     * and before returning the queue of messages that came before the service started is drained.
     *
     * @param master flag indicating if the current node is master
     */
    public void start(boolean master) {
        if (state.compareAndSet(State.STOPPED, State.STARTING)) {
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

            initializeClient();
            if (master) {
                putTemplate(customAuditIndexSettings(settings));
            }
            initializeBulkProcessor();
            state.set(State.STARTED);
            drainQueue();
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                bulkProcessor.flush();
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    public void close() {
        if (state.get() != State.STOPPED) {
            stop();
        }

        try {
            bulkProcessor.close();
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
                            @Nullable String realm, @Nullable String[] indices, TransportMessage message) throws Exception {

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
            msg.builder.array(Field.INDICES, indices);
        }
        if (logger.isDebugEnabled()) {
            msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());
        }

        return msg.end();
    }

    private Message message(String type, @Nullable String action, @Nullable String principal,
                            @Nullable String realm, @Nullable String[] indices, RestRequest request) throws Exception {

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
            msg.builder.array(Field.INDICES, indices);
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
            builder.field(Field.ORIGIN_ADDRESS, restAddress.getAddress().getHostAddress());
            return builder;
        }

        // we'll see if was originated in a remote node
        TransportAddress address = message.remoteAddress();
        if (address != null) {
            builder.field(Field.ORIGIN_TYPE, "transport");
            if (address instanceof InetSocketTransportAddress) {
                builder.field(Field.ORIGIN_ADDRESS, ((InetSocketTransportAddress) address).address().getAddress().getHostAddress());
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

    void submit(Message message) {
        if (state.get() != State.STARTED) {
            eventQueue.add(message);
            return;
        }

        IndexRequest indexRequest = client.prepareIndex()
                .setIndex(resolve(INDEX_NAME_PREFIX, message.timestamp, rollover))
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

    Settings customAuditIndexSettings(Settings nodeSettings) {
        Settings newSettings = Settings.builder()
                .put(nodeSettings.getAsSettings("shield.audit.index.settings.index"))
                .build();
        if (newSettings.names().isEmpty()) {
            return Settings.EMPTY;
        }

        // Filter out forbidden settings:
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, String> entry : newSettings.getAsMap().entrySet()) {
            String name = "index." + entry.getKey();
            if (forbiddenIndexSettings.contains(name)) {
                logger.warn("overriding the default [{}} setting is forbidden. ignoring...", name);
                continue;
            }
            builder.put(name, entry.getValue());
        }
        return builder.build();
    }

    void putTemplate(Settings customSettings) {
        try {
            final byte[] template = Streams.copyToBytesFromClasspath("/" + INDEX_TEMPLATE_NAME + ".json");
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME).source(template);
            if (customSettings != null && customSettings.names().size() > 0) {
                Settings updatedSettings = Settings.builder()
                        .put(request.settings())
                        .put(customSettings)
                        .build();
                request.settings(updatedSettings);
            }

            authenticationService.attachUserHeaderIfMissing(request, auditUser.user());
            assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";
            PutIndexTemplateResponse response = client.admin().indices().putTemplate(request).actionGet();
            if (!response.isAcknowledged()) {
                throw new ShieldException("failed to put index template for audit logging");
            }
        } catch (Exception e) {
            logger.debug("unexpected exception while putting index template", e);
            throw new ShieldException("failed to load [" + INDEX_TEMPLATE_NAME + ".json]", e);
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

    private void drainQueue() {
        Message message = eventQueue.poll();
        while (message != null) {
            submit(message);
            message = eventQueue.poll();
        }
    }

    static class Message {

        final DateTime timestamp;
        final XContentBuilder builder;

        Message() throws IOException {
            this.timestamp = DateTime.now(DateTimeZone.UTC);
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
        XContentBuilderString TIMESTAMP = new XContentBuilderString("@timestamp");
        XContentBuilderString NODE_NAME = new XContentBuilderString("node_name");
        XContentBuilderString NODE_HOST_NAME = new XContentBuilderString("node_host_name");
        XContentBuilderString NODE_HOST_ADDRESS = new XContentBuilderString("node_host_address");
        XContentBuilderString LAYER = new XContentBuilderString("layer");
        XContentBuilderString TYPE = new XContentBuilderString("event_type");
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

    public enum State {
        STOPPED,
        STARTING,
        STARTED,
        STOPPING
    }
}
