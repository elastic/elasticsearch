/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.admin.ShieldInternalUserHolder;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.rest.RemoteHostHeader;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.XPackPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.shield.audit.AuditUtil.indices;
import static org.elasticsearch.shield.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.ACCESS_DENIED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.ACCESS_GRANTED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.ANONYMOUS_ACCESS_DENIED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.AUTHENTICATION_FAILED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.CONNECTION_DENIED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.CONNECTION_GRANTED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.RUN_AS_DENIED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.RUN_AS_GRANTED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.SYSTEM_ACCESS_GRANTED;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.TAMPERED_REQUEST;
import static org.elasticsearch.shield.audit.index.IndexAuditLevel.parse;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.resolve;

/**
 * Audit trail implementation that writes events into an index.
 */
public class IndexAuditTrail extends AbstractComponent implements AuditTrail, ClusterStateListener {

    public static final int DEFAULT_BULK_SIZE = 1000;
    public static final int MAX_BULK_SIZE = 10000;
    public static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
    public static final TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(1);
    public static final IndexNameResolver.Rollover DEFAULT_ROLLOVER = IndexNameResolver.Rollover.DAILY;
    public static final String NAME = "index";
    public static final String INDEX_NAME_PREFIX = ".shield_audit_log";
    public static final String DOC_TYPE = "event";
    public static final String ROLLOVER_SETTING = "shield.audit.index.rollover";
    public static final String QUEUE_SIZE_SETTING = "shield.audit.index.queue_max_size";
    public static final String INDEX_TEMPLATE_NAME = "shield_audit_log";
    public static final String DEFAULT_CLIENT_NAME = "shield-audit-client";

    static final String[] DEFAULT_EVENT_INCLUDES = new String[] {
            ACCESS_DENIED.toString(),
            ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(),
            AUTHENTICATION_FAILED.toString(),
            CONNECTION_DENIED.toString(),
            CONNECTION_GRANTED.toString(),
            TAMPERED_REQUEST.toString(),
            RUN_AS_DENIED.toString(),
            RUN_AS_GRANTED.toString()
    };

    private static final String FORBIDDEN_INDEX_SETTING = "index.mapper.dynamic";

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final String nodeName;
    private final IndexAuditUserHolder auditUser;
    private final Provider<Client> clientProvider;
    private final AuthenticationService authenticationService;
    private final LinkedBlockingQueue<Message> eventQueue;
    private final QueueConsumer queueConsumer;
    private final Transport transport;
    private final ThreadPool threadPool;
    private final Lock putMappingLock = new ReentrantLock();
    private final ClusterService clusterService;
    private final boolean indexToRemoteCluster;

    private BulkProcessor bulkProcessor;
    private Client client;
    private IndexNameResolver.Rollover rollover;
    private String nodeHostName;
    private String nodeHostAddress;
    private EnumSet<IndexAuditLevel> events;

    @Override
    public String name() {
        return NAME;
    }

    @Inject
    public IndexAuditTrail(Settings settings, IndexAuditUserHolder indexingAuditUser,
                           AuthenticationService authenticationService, Transport transport,
                           Provider<Client> clientProvider, ThreadPool threadPool, ClusterService clusterService) {
        super(settings);
        this.auditUser = indexingAuditUser;
        this.authenticationService = authenticationService;
        this.clientProvider = clientProvider;
        this.transport = transport;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.nodeName = settings.get("name");
        this.queueConsumer = new QueueConsumer(EsExecutors.threadName(settings, "audit-queue-consumer"));

        int maxQueueSize = settings.getAsInt(QUEUE_SIZE_SETTING, DEFAULT_MAX_QUEUE_SIZE);
        if (maxQueueSize <= 0) {
            logger.warn("invalid value [{}] for setting [{}]. using default value [{}]", maxQueueSize, QUEUE_SIZE_SETTING, DEFAULT_MAX_QUEUE_SIZE);
            maxQueueSize = DEFAULT_MAX_QUEUE_SIZE;
        }
        this.eventQueue = new LinkedBlockingQueue<>(maxQueueSize);

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
        } catch (IllegalArgumentException e) {
            logger.warn("invalid event type specified, using default for audit index output. include events [{}], exclude events [{}]", e, includedEvents, excludedEvents);
            events = parse(DEFAULT_EVENT_INCLUDES, Strings.EMPTY_ARRAY);
        }
        this.indexToRemoteCluster = settings.getByPrefix("shield.audit.index.client.").names().size() > 0;

    }

    public State state() {
        return state.get();
    }

    /**
     * This method determines if this service can be started based on the state in the {@link ClusterChangedEvent} and
     * if the node is the master or not. When using remote indexing, a call to the remote cluster will be made to retrieve
     * the state and the same rules will be applied. In order for the service to start, the following must be true:
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
    public synchronized boolean canStart(ClusterChangedEvent event, boolean master) {
        if (indexToRemoteCluster) {
            try {
                if (client == null) {
                    initializeClient();
                }
            } catch (Exception e) {
                logger.error("failed to initialize client for remote indexing. index based output is disabled", e);
                state.set(State.FAILED);
                return false;
            }

            ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
            return canStart(response.getState(), master);
        }
        return canStart(event.state(), master);
    }

    private boolean canStart(ClusterState clusterState, boolean master) {
        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .shield-audit-
            // but they may not have been restored from the cluster state on disk
            logger.debug("index audit trail waiting until gateway has recovered from disk");
            return false;
        }

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
        if (state.compareAndSet(State.INITIALIZED, State.STARTING)) {
            this.nodeHostName = transport.boundAddress().publishAddress().getHost();
            this.nodeHostAddress = transport.boundAddress().publishAddress().getAddress();

            if (client == null) {
                initializeClient();
            }

            if (master) {
                putTemplate(customAuditIndexSettings(settings));
            }
            this.clusterService.add(this);
            initializeBulkProcessor();
            queueConsumer.start();
            state.set(State.STARTED);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                queueConsumer.interrupt();
                if (bulkProcessor != null) {
                    bulkProcessor.flush();
                }
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
                enqueue(message("anonymous_access_denied", action, null, null, indices(message), message), "anonymous_access_denied");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [anonymous_access_denied]", e);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            try {
                enqueue(message("anonymous_access_denied", null, null, null, null, request), "anonymous_access_denied");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [anonymous_access_denied]", e);
            }
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage<?> message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            try {
                enqueue(message("authentication_failed", action, null, null, indices(message), message), "authentication_failed");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [authentication_failed]", e);
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            try {
                enqueue(message("authentication_failed", null, null, null, null, request), "authentication_failed");
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
                    enqueue(message("authentication_failed", action, token, null, indices(message), message), "authentication_failed");
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
                    enqueue(message("authentication_failed", null, token, null, null, request), "authentication_failed");
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
                    enqueue(message("authentication_failed", action, token, realm, indices(message), message), "authentication_failed");
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
                    enqueue(message("authentication_failed", null, token, realm, null, request), "authentication_failed");
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
            if ((user.isSystem() && Privilege.SYSTEM.predicate().test(action)) || ShieldInternalUserHolder.isShieldInternalUser(user)) {
                if (events.contains(SYSTEM_ACCESS_GRANTED)) {
                    try {
                        enqueue(message("access_granted", action, user, indices(message), message), "access_granted");
                    } catch (Exception e) {
                        logger.warn("failed to index audit event: [access_granted]", e);
                    }
                }
            } else if (events.contains(ACCESS_GRANTED)) {
                try {
                    enqueue(message("access_granted", action, user, indices(message), message), "access_granted");
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
                    enqueue(message("access_denied", action, user, indices(message), message), "access_denied");
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [access_denied]", e);
                }
            }
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage<?> message) {
        if (events.contains(TAMPERED_REQUEST)) {
            try {
                enqueue(message("tampered_request", action, null, indices(message), message), "tampered_request");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [tampered_request]", e);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage<?> request) {
        if (events.contains(TAMPERED_REQUEST)) {
            if (!principalIsAuditor(user.principal())) {
                try {
                    enqueue(message("tampered_request", action, user, indices(request), request), "tampered_request");
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
                enqueue(message("ip_filter", "connection_granted", inetAddress, profile, rule), "connection_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_granted]", e);
            }
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED)) {
            try {
                enqueue(message("ip_filter", "connection_denied", inetAddress, profile, rule), "connection_denied");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_denied]", e);
            }
        }
    }

    @Override
    public void runAsGranted(User user, String action, TransportMessage<?> message) {
        if (events.contains(RUN_AS_GRANTED)) {
            try {
                enqueue(message("run_as_granted", action, user, null, message), "access_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_granted]", e);
            }
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage<?> message) {
        if (events.contains(RUN_AS_DENIED)) {
            try {
                enqueue(message("run_as_denied", action, user, null, message), "access_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_denied]", e);
            }
        }
    }

    private boolean principalIsAuditor(String principal) {
        return (principal.equals(auditUser.user().principal()));
    }

    private Message message(String type, @Nullable String action, @Nullable User user,
                            @Nullable String[] indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder, transport);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }
        if (user != null) {
            if (user.runAs() != null) {
                if ("run_as_granted".equals(type) || "run_as_denied".equals(type)) {
                    msg.builder.field(Field.PRINCIPAL, user.principal());
                    msg.builder.field(Field.RUN_AS_PRINCIPAL, user.runAs().principal());
                } else {
                    msg.builder.field(Field.PRINCIPAL, user.runAs().principal());
                    msg.builder.field(Field.RUN_BY_PRINCIPAL, user.principal());
                }
            } else {
                msg.builder.field(Field.PRINCIPAL, user.principal());
            }
        }
        if (indices != null) {
            msg.builder.array(Field.INDICES, indices);
        }
        msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());

        return msg.end();
    }

    // FIXME - clean up the message generation
    private Message message(String type, @Nullable String action, @Nullable AuthenticationToken token,
                            @Nullable String realm, @Nullable String[] indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder, transport);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }
        if (token != null) {
            msg.builder.field(Field.PRINCIPAL, token.principal());
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (indices != null) {
            msg.builder.array(Field.INDICES, indices);
        }
        msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());

        return msg.end();
    }

    private Message message(String type, @Nullable String action, @Nullable AuthenticationToken token,
                            @Nullable String realm, @Nullable String[] indices, RestRequest request) throws Exception {

        Message msg = new Message().start();
        common("rest", type, msg.builder);

        if (action != null) {
            msg.builder.field(Field.ACTION, action);
        }

        if (token != null) {
            msg.builder.field(Field.PRINCIPAL, token.principal());
        }

        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (indices != null) {
            msg.builder.array(Field.INDICES, indices);
        }
        msg.builder.field(Field.REQUEST_BODY, restRequestContent(request));
        msg.builder.field(Field.ORIGIN_TYPE, "rest");
        SocketAddress address = request.getRemoteAddress();
        if (address instanceof InetSocketAddress) {
            msg.builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.formatAddress(((InetSocketAddress) request.getRemoteAddress()).getAddress()));
        } else {
            msg.builder.field(Field.ORIGIN_ADDRESS, address);
        }
        msg.builder.field(Field.URI, request.uri());

        return msg.end();
    }

    private Message message(String layer, String type, InetAddress originAddress, String profile,
                            ShieldIpFilterRule rule) throws IOException {

        Message msg = new Message().start();
        common(layer, type, msg.builder);

        msg.builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.formatAddress(originAddress));
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

    private static XContentBuilder originAttributes(TransportMessage message, XContentBuilder builder, Transport transport) throws IOException {

        // first checking if the message originated in a rest call
        InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(message);
        if (restAddress != null) {
            builder.field(Field.ORIGIN_TYPE, "rest");
            builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.formatAddress(restAddress.getAddress()));
            return builder;
        }

        // we'll see if was originated in a remote node
        TransportAddress address = message.remoteAddress();
        if (address != null) {
            builder.field(Field.ORIGIN_TYPE, "transport");
            if (address instanceof InetSocketTransportAddress) {
                builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.formatAddress(((InetSocketTransportAddress) address).address().getAddress()));
            } else {
                builder.field(Field.ORIGIN_ADDRESS, address);
            }
            return builder;
        }

        // the call was originated locally on this node
        builder.field(Field.ORIGIN_TYPE, "local_node");
        builder.field(Field.ORIGIN_ADDRESS, transport.boundAddress().publishAddress().getAddress());
        return builder;
    }

    void enqueue(Message message, String type) {
        State currentState = state();
        if (currentState != State.STOPPING && currentState != State.STOPPED) {
            boolean accepted = eventQueue.offer(message);
            if (!accepted) {
                logger.warn("failed to index audit event: [{}]. queue is full; bulk processor may not be able to keep up or has stopped indexing.", type);
            }
        }
    }

    // for testing to ensure we get the proper timestamp and index name...
    Message peek() {
        return eventQueue.peek();
    }

    private void initializeClient() {
        if (indexToRemoteCluster == false) {
            // in the absence of client settings for remote indexing, fall back to the client that was passed in.
            this.client = clientProvider.get();
        } else {
            Settings clientSettings = settings.getByPrefix("shield.audit.index.client.");
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
                List<String> hostPort = Arrays.asList(host.trim().split(":"));
                if (hostPort.size() != 1 && hostPort.size() != 2) {
                    logger.warn("invalid host:port specified: [{}] for setting [shield.audit.index.client.hosts]", host);
                }
                hostPortPairs.add(new Tuple<>(hostPort.get(0), hostPort.size() == 2 ? Integer.valueOf(hostPort.get(1)) : 9300));
            }

            if (hostPortPairs.size() == 0) {
                throw new ElasticsearchException("no valid host:port pairs specified for setting [shield.audit.index.client.hosts]");
            }

            final TransportClient transportClient = TransportClient.builder()
                    .settings(Settings.builder()
                            .put("name", DEFAULT_CLIENT_NAME + "-" + settings.get("name"))
                            .put(clientSettings))
                    .addPlugin(XPackPlugin.class)
                    .build();
            for (Tuple<String, Integer> pair : hostPortPairs) {
                try {
                    transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(pair.v1()), pair.v2()));
                } catch (UnknownHostException e) {
                    throw new ElasticsearchException("could not find host {}", e, pair.v1());
                }
            }

            this.client = transportClient;
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
            if (FORBIDDEN_INDEX_SETTING.equals(name)) {
                logger.warn("overriding the default [{}} setting is forbidden. ignoring...", name);
                continue;
            }
            builder.put(name, entry.getValue());
        }
        return builder.build();
    }

    void putTemplate(Settings customSettings) {
        try (InputStream is = getClass().getResourceAsStream("/" + INDEX_TEMPLATE_NAME + ".json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            final byte[] template = out.toByteArray();
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME).source(template);
            if (customSettings != null && customSettings.names().size() > 0) {
                Settings updatedSettings = Settings.builder()
                        .put(request.settings())
                        .put(customSettings)
                        .build();
                request.settings(updatedSettings);
            }

            assert !Thread.currentThread().isInterrupted() : "current thread has been interrupted before putting index template!!!";

            if (!indexToRemoteCluster) {
                authenticationService.attachUserHeaderIfMissing(request, auditUser.user());
            }
            PutIndexTemplateResponse response = client.admin().indices().putTemplate(request).actionGet();
            if (!response.isAcknowledged()) {
                throw new IllegalStateException("failed to put index template for audit logging");
            }

            // now we may need to update the mappings of the current index
            DateTime dateTime;
            Message message = eventQueue.peek();
            if (message != null) {
                dateTime = message.timestamp;
            } else {
                dateTime = DateTime.now(DateTimeZone.UTC);
            }
            String index = resolve(INDEX_NAME_PREFIX, dateTime, rollover);
            IndicesExistsRequest existsRequest = new IndicesExistsRequest(index);
            // TODO need to clean this up so we don't forget to attach the header...
            if (!indexToRemoteCluster) {
                authenticationService.attachUserHeaderIfMissing(existsRequest, auditUser.user());
            }

            if (client.admin().indices().exists(existsRequest).get().isExists()) {
                logger.debug("index [{}] exists so we need to update mappings", index);
                PutMappingRequest putMappingRequest = new PutMappingRequest(index).type(DOC_TYPE).source(request.mappings().get(DOC_TYPE));
                if (!indexToRemoteCluster) {
                    authenticationService.attachUserHeaderIfMissing(putMappingRequest, auditUser.user());
                }

                PutMappingResponse putMappingResponse = client.admin().indices().putMapping(putMappingRequest).get();
                if (!putMappingResponse.isAcknowledged()) {
                    throw new IllegalStateException("failed to put mappings for audit logging index [" + index + "]");
                }
            } else {
                logger.debug("index [{}] does not exist so we do not need to update mappings", index);
            }
        } catch (Exception e) {
            logger.debug("unexpected exception while putting index template", e);
            throw new IllegalStateException("failed to load [" + INDEX_TEMPLATE_NAME + ".json]", e);
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
                try {
                    if (!indexToRemoteCluster) {
                        authenticationService.attachUserHeaderIfMissing(request, auditUser.user());
                    }
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to attach user header", e);
                }
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

    // this could be handled by a template registry service but adding that is extra complexity until we actually need it
    @Override
    public void clusterChanged(ClusterChangedEvent clusterChangedEvent) {
        State state = state();
        if (state != State.STARTED || indexToRemoteCluster) {
            return;
        }

        if (clusterChangedEvent.localNodeMaster() == false) {
            return;
        }
        if (clusterChangedEvent.state().metaData().templates().get(INDEX_TEMPLATE_NAME) == null) {
            logger.debug("shield audit index template [{}] does not exist. it may have been deleted - putting the template", INDEX_TEMPLATE_NAME);
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable throwable) {
                    logger.error("failed to update shield audit index template [{}]", throwable, INDEX_TEMPLATE_NAME);
                }

                @Override
                protected void doRun() throws Exception {
                    final boolean locked = putMappingLock.tryLock();
                    if (locked) {
                        try {
                            putTemplate(customAuditIndexSettings(settings));
                        } finally {
                            putMappingLock.unlock();
                        }
                    } else {
                        logger.trace("unable to PUT shield audit index template as the lock is already held");
                    }
                }
            });
        }
    }

    private class QueueConsumer extends Thread {

        volatile boolean running = true;

        QueueConsumer(String name) {
            super(name);
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Message message = eventQueue.take();
                    IndexRequest indexRequest = client.prepareIndex()
                            .setIndex(resolve(INDEX_NAME_PREFIX, message.timestamp, rollover))
                            .setType(DOC_TYPE).setSource(message.builder).request();
                    if (!indexToRemoteCluster) {
                        authenticationService.attachUserHeaderIfMissing(indexRequest, auditUser.user());
                    }
                    bulkProcessor.add(indexRequest);
                } catch (InterruptedException e) {
                    logger.debug("index audit queue consumer interrupted", e);
                    running = false;
                    return;
                } catch (Exception e) {
                    // log the exception and keep going
                    logger.warn("failed to index audit message from queue", e);
                }
            }
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
        XContentBuilderString RUN_AS_PRINCIPAL = new XContentBuilderString("run_as_principal");
        XContentBuilderString RUN_BY_PRINCIPAL = new XContentBuilderString("run_by_principal");
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
        INITIALIZED,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }
}
