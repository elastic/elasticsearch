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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.user.XPackUser;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authz.privilege.SystemPrivilege;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

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
import static org.elasticsearch.shield.Security.setting;

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
    public static final Setting<IndexNameResolver.Rollover> ROLLOVER_SETTING =
            new Setting<>(setting("audit.index.rollover"), (s) -> DEFAULT_ROLLOVER.name(),
                    s -> IndexNameResolver.Rollover.valueOf(s.toUpperCase(Locale.ENGLISH)), Property.NodeScope);
    public static final Setting<Integer> QUEUE_SIZE_SETTING =
            Setting.intSetting(setting("audit.index.queue_max_size"), DEFAULT_MAX_QUEUE_SIZE, 1, Property.NodeScope);
    public static final String INDEX_TEMPLATE_NAME = "shield_audit_log";
    public static final String DEFAULT_CLIENT_NAME = "shield-audit-client";

    static final List<String> DEFAULT_EVENT_INCLUDES = Arrays.asList(
            ACCESS_DENIED.toString(),
            ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(),
            AUTHENTICATION_FAILED.toString(),
            CONNECTION_DENIED.toString(),
            CONNECTION_GRANTED.toString(),
            TAMPERED_REQUEST.toString(),
            RUN_AS_DENIED.toString(),
            RUN_AS_GRANTED.toString()
    );
    private static final String FORBIDDEN_INDEX_SETTING = "index.mapper.dynamic";

    public static final Setting<Settings> INDEX_SETTINGS =
            Setting.groupSetting(setting("audit.index.settings.index."), Property.NodeScope);
    public static final Setting<List<String>> INCLUDE_EVENT_SETTINGS =
            Setting.listSetting(setting("audit.index.events.include"), DEFAULT_EVENT_INCLUDES, Function.identity(),
                    Property.NodeScope);
    public static final Setting<List<String>> EXCLUDE_EVENT_SETTINGS =
            Setting.listSetting(setting("audit.index.events.exclude"), Collections.emptyList(),
                    Function.identity(), Property.NodeScope);
    public static final Setting<Settings> REMOTE_CLIENT_SETTINGS =
            Setting.groupSetting(setting("audit.index.client."), Property.NodeScope);
    public static final Setting<Integer> BULK_SIZE_SETTING =
            Setting.intSetting(setting("audit.index.bulk_size"), DEFAULT_BULK_SIZE, 1, MAX_BULK_SIZE, Property.NodeScope);
    public static final Setting<TimeValue> FLUSH_TIMEOUT_SETTING =
            Setting.timeSetting(setting("audit.index.flush_interval"), DEFAULT_FLUSH_INTERVAL,
                    TimeValue.timeValueMillis(1L), Property.NodeScope);


    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final String nodeName;
    private final Provider<InternalClient> clientProvider;
    private final BlockingQueue<Message> eventQueue;
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
    public IndexAuditTrail(Settings settings, Transport transport,
                           Provider<InternalClient> clientProvider, ThreadPool threadPool, ClusterService clusterService) {
        super(settings);
        this.clientProvider = clientProvider;
        this.transport = transport;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.nodeName = settings.get("name");
        this.queueConsumer = new QueueConsumer(EsExecutors.threadName(settings, "audit-queue-consumer"));
        int maxQueueSize = QUEUE_SIZE_SETTING.get(settings);
        this.eventQueue = createQueue(maxQueueSize);

        // we have to initialize this here since we use rollover in determining if we can start...
        rollover = ROLLOVER_SETTING.get(settings);

        // we have to initialize the events here since we can receive events before starting...
        List<String> includedEvents = INCLUDE_EVENT_SETTINGS.get(settings);
        List<String> excludedEvents = EXCLUDE_EVENT_SETTINGS.get(settings);
        try {
            events = parse(includedEvents, excludedEvents);
        } catch (IllegalArgumentException e) {
            logger.warn("invalid event type specified, using default for audit index output. include events [{}], exclude events [{}]",
                    e, includedEvents, excludedEvents);
            events = parse(DEFAULT_EVENT_INCLUDES, Collections.emptyList());
        }
        this.indexToRemoteCluster = REMOTE_CLIENT_SETTINGS.get(settings).names().size() > 0;

    }

    public State state() {
        return state.get();
    }

    /**
     * This method determines if this service can be started based on the state in the {@link ClusterChangedEvent} and
     * if the node is the master or not. When using remote indexing, a call to the remote cluster will be made to retrieve
     * the state and the same rules will be applied. In order for the service to start, the following must be true:
     * <ol>
     * <li>The cluster must not have a {@link GatewayService#STATE_NOT_RECOVERED_BLOCK}; in other words the gateway
     * must have recovered from disk already.</li>
     * <li>The current node must be the master OR the <code>shield_audit_log</code> index template must exist</li>
     * <li>The current audit index must not exist or have all primary shards active. The current audit index name
     * is determined by the rollover settings and current time</li>
     * </ol>
     *
     * @param event  the {@link ClusterChangedEvent} containing the up to date cluster state
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
    public void anonymousAccessDenied(String action, TransportMessage message) {
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
    public void authenticationFailed(String action, TransportMessage message) {
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
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (XPackUser.is(token.principal()) == false) {
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
            if (XPackUser.is(token.principal()) == false) {
                try {
                    enqueue(message("authentication_failed", null, token, null, null, request), "authentication_failed");
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (XPackUser.is(token.principal()) == false) {
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
            if (XPackUser.is(token.principal()) == false) {
                try {
                    enqueue(message("authentication_failed", null, token, realm, null, request), "authentication_failed");
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage message) {
        // special treatment for internal system actions - only log if explicitly told to
        if ((SystemUser.is(user) && SystemPrivilege.INSTANCE.predicate().test(action))) {
            if (events.contains(SYSTEM_ACCESS_GRANTED)) {
                try {
                    enqueue(message("access_granted", action, user, indices(message), message), "access_granted");
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [access_granted]", e);
                }
            }
        } else if (events.contains(ACCESS_GRANTED) && XPackUser.is(user) == false) {
            try {
                enqueue(message("access_granted", action, user, indices(message), message), "access_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [access_granted]", e);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message) {
        if (events.contains(ACCESS_DENIED) && XPackUser.is(user) == false) {
            try {
                enqueue(message("access_denied", action, user, indices(message), message), "access_denied");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [access_denied]", e);
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        if (events.contains(TAMPERED_REQUEST)) {
            try {
                enqueue(message("tampered_request", null, null, null, null, request), "tampered_request");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [tampered_request]", e);
            }
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        if (events.contains(TAMPERED_REQUEST)) {
            try {
                enqueue(message("tampered_request", action, null, indices(message), message), "tampered_request");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [tampered_request]", e);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        if (events.contains(TAMPERED_REQUEST) && XPackUser.is(user) == false) {
            try {
                enqueue(message("tampered_request", action, user, indices(request), request), "tampered_request");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [tampered_request]", e);
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
    public void runAsGranted(User user, String action, TransportMessage message) {
        if (events.contains(RUN_AS_GRANTED)) {
            try {
                enqueue(message("run_as_granted", action, user, null, message), "access_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_granted]", e);
            }
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (events.contains(RUN_AS_DENIED)) {
            try {
                enqueue(message("run_as_denied", action, user, null, message), "access_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_denied]", e);
            }
        }
    }

    private Message message(String type, @Nullable String action, @Nullable User user,
                            @Nullable String[] indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder, transport, threadPool.getThreadContext());

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
        originAttributes(message, msg.builder, transport, threadPool.getThreadContext());

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
            msg.builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.format(((InetSocketAddress) request.getRemoteAddress())
                    .getAddress()));
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

        msg.builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.format(originAddress));
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

    private static XContentBuilder originAttributes(TransportMessage message, XContentBuilder builder, Transport transport, ThreadContext
            threadContext) throws IOException {

        // first checking if the message originated in a rest call
        InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress != null) {
            builder.field(Field.ORIGIN_TYPE, "rest");
            builder.field(Field.ORIGIN_ADDRESS, NetworkAddress.format(restAddress.getAddress()));
            return builder;
        }

        // we'll see if was originated in a remote node
        TransportAddress address = message.remoteAddress();
        if (address != null) {
            builder.field(Field.ORIGIN_TYPE, "transport");
            if (address instanceof InetSocketTransportAddress) {
                builder.field(Field.ORIGIN_ADDRESS,
                        NetworkAddress.format(((InetSocketTransportAddress) address).address().getAddress()));
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
                logger.warn("failed to index audit event: [{}]. queue is full; bulk processor may not be able to keep up" +
                        "or has stopped indexing.", type);
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
            Settings clientSettings = REMOTE_CLIENT_SETTINGS.get(settings);
            String[] hosts = clientSettings.getAsArray("hosts");
            if (hosts.length == 0) {
                throw new ElasticsearchException("missing required setting " +
                        "[" + REMOTE_CLIENT_SETTINGS.getKey() + ".hosts] for remote audit log indexing");
            }

            if (clientSettings.get("cluster.name", "").isEmpty()) {
                throw new ElasticsearchException("missing required setting " +
                        "[" + REMOTE_CLIENT_SETTINGS.getKey() + ".cluster.name] for remote audit log indexing");
            }

            List<Tuple<String, Integer>> hostPortPairs = new ArrayList<>();

            for (String host : hosts) {
                List<String> hostPort = Arrays.asList(host.trim().split(":"));
                if (hostPort.size() != 1 && hostPort.size() != 2) {
                    logger.warn("invalid host:port specified: [{}] for setting [" + REMOTE_CLIENT_SETTINGS.getKey() + ".hosts]", host);
                }
                hostPortPairs.add(new Tuple<>(hostPort.get(0), hostPort.size() == 2 ? Integer.valueOf(hostPort.get(1)) : 9300));
            }

            if (hostPortPairs.size() == 0) {
                throw new ElasticsearchException("no valid host:port pairs specified for setting ["
                        + REMOTE_CLIENT_SETTINGS.getKey() + ".hosts]");
            }
            final Settings theClientSetting = clientSettings.filter((s) -> s.startsWith("hosts") == false); // hosts is not a valid setting
            final TransportClient transportClient = TransportClient.builder()
                    .settings(Settings.builder()
                            .put("node.name", DEFAULT_CLIENT_NAME + "-" + Node.NODE_NAME_SETTING.get(settings))
                            .put(theClientSetting))
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
                .put(INDEX_SETTINGS.get(nodeSettings))
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

            if (client.admin().indices().exists(existsRequest).get().isExists()) {
                logger.debug("index [{}] exists so we need to update mappings", index);
                PutMappingRequest putMappingRequest = new PutMappingRequest(index).type(DOC_TYPE).source(request.mappings().get(DOC_TYPE));
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

    BlockingQueue<Message> createQueue(int maxQueueSize) {
        return new LinkedBlockingQueue<>(maxQueueSize);
    }

    private void initializeBulkProcessor() {

        final int bulkSize = BULK_SIZE_SETTING.get(settings);
        final TimeValue interval = FLUSH_TIMEOUT_SETTING.get(settings);

        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
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
            logger.debug("shield audit index template [{}] does not exist. it may have been deleted - putting the template",
                    INDEX_TEMPLATE_NAME);
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

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(INDEX_SETTINGS);
        settingsModule.registerSetting(EXCLUDE_EVENT_SETTINGS);
        settingsModule.registerSetting(INCLUDE_EVENT_SETTINGS);
        settingsModule.registerSetting(ROLLOVER_SETTING);
        settingsModule.registerSetting(BULK_SIZE_SETTING);
        settingsModule.registerSetting(FLUSH_TIMEOUT_SETTING);
        settingsModule.registerSetting(QUEUE_SIZE_SETTING);
        settingsModule.registerSetting(REMOTE_CLIENT_SETTINGS);
    }

    private class QueueConsumer extends Thread {

        volatile boolean running = true;

        QueueConsumer(String name) {
            super(name);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Message message = eventQueue.take();
                    IndexRequest indexRequest = client.prepareIndex()
                            .setIndex(resolve(INDEX_NAME_PREFIX, message.timestamp, rollover))
                            .setType(DOC_TYPE).setSource(message.builder).request();
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
        String TIMESTAMP = "@timestamp";
        String NODE_NAME = "node_name";
        String NODE_HOST_NAME = "node_host_name";
        String NODE_HOST_ADDRESS = "node_host_address";
        String LAYER = "layer";
        String TYPE = "event_type";
        String ORIGIN_ADDRESS = "origin_address";
        String ORIGIN_TYPE = "origin_type";
        String PRINCIPAL = "principal";
        String RUN_AS_PRINCIPAL = "run_as_principal";
        String RUN_BY_PRINCIPAL = "run_by_principal";
        String ACTION = "action";
        String INDICES = "indices";
        String REQUEST = "request";
        String REQUEST_BODY = "request_body";
        String URI = "uri";
        String REALM = "realm";
        String TRANSPORT_PROFILE = "transport_profile";
        String RULE = "rule";
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
