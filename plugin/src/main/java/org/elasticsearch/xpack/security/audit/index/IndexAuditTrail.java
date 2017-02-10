/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authz.privilege.SystemPrivilege;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.audit.AuditLevel.ACCESS_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.ACCESS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.ANONYMOUS_ACCESS_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.AUTHENTICATION_FAILED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.AUTHENTICATION_SUCCESS;
import static org.elasticsearch.xpack.security.audit.AuditLevel.CONNECTION_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.CONNECTION_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.REALM_AUTHENTICATION_FAILED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.RUN_AS_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.RUN_AS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.SYSTEM_ACCESS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.TAMPERED_REQUEST;
import static org.elasticsearch.xpack.security.audit.AuditLevel.parse;
import static org.elasticsearch.xpack.security.audit.AuditUtil.indices;
import static org.elasticsearch.xpack.security.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.resolve;

/**
 * Audit trail implementation that writes events into an index.
 */
public class IndexAuditTrail extends AbstractComponent implements AuditTrail, ClusterStateListener {

    public static final String NAME = "index";
    public static final String INDEX_NAME_PREFIX = ".security_audit_log";
    public static final String DOC_TYPE = "event";
    public static final String INDEX_TEMPLATE_NAME = "security_audit_log";

    private static final int DEFAULT_BULK_SIZE = 1000;
    private static final int MAX_BULK_SIZE = 10000;
    private static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
    private static final TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(1);
    private static final IndexNameResolver.Rollover DEFAULT_ROLLOVER = IndexNameResolver.Rollover.DAILY;
    private static final Setting<IndexNameResolver.Rollover> ROLLOVER_SETTING =
            new Setting<>(setting("audit.index.rollover"), (s) -> DEFAULT_ROLLOVER.name(),
                    s -> IndexNameResolver.Rollover.valueOf(s.toUpperCase(Locale.ENGLISH)), Property.NodeScope);
    private static final Setting<Integer> QUEUE_SIZE_SETTING =
            Setting.intSetting(setting("audit.index.queue_max_size"), DEFAULT_MAX_QUEUE_SIZE, 1, Property.NodeScope);
    private static final String DEFAULT_CLIENT_NAME = "security-audit-client";

    private static final List<String> DEFAULT_EVENT_INCLUDES = Arrays.asList(
            ACCESS_DENIED.toString(),
            ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(),
            AUTHENTICATION_FAILED.toString(),
            REALM_AUTHENTICATION_FAILED.toString(),
            CONNECTION_DENIED.toString(),
            CONNECTION_GRANTED.toString(),
            TAMPERED_REQUEST.toString(),
            RUN_AS_DENIED.toString(),
            RUN_AS_GRANTED.toString(),
            AUTHENTICATION_SUCCESS.toString()
    );
    private static final String FORBIDDEN_INDEX_SETTING = "index.mapper.dynamic";

    private static final Setting<Settings> INDEX_SETTINGS =
            Setting.groupSetting(setting("audit.index.settings.index."), Property.NodeScope);
    private static final Setting<List<String>> INCLUDE_EVENT_SETTINGS =
            Setting.listSetting(setting("audit.index.events.include"), DEFAULT_EVENT_INCLUDES, Function.identity(),
                    Property.NodeScope);
    private static final Setting<List<String>> EXCLUDE_EVENT_SETTINGS =
            Setting.listSetting(setting("audit.index.events.exclude"), Collections.emptyList(),
                    Function.identity(), Property.NodeScope);
    private static final Setting<Boolean> INCLUDE_REQUEST_BODY =
            Setting.boolSetting(setting("audit.index.events.emit_request_body"), false, Property.NodeScope);
    private static final Setting<Settings> REMOTE_CLIENT_SETTINGS =
            Setting.groupSetting(setting("audit.index.client."), Property.NodeScope);
    private static final Setting<Integer> BULK_SIZE_SETTING =
            Setting.intSetting(setting("audit.index.bulk_size"), DEFAULT_BULK_SIZE, 1, MAX_BULK_SIZE, Property.NodeScope);
    private static final Setting<TimeValue> FLUSH_TIMEOUT_SETTING =
            Setting.timeSetting(setting("audit.index.flush_interval"), DEFAULT_FLUSH_INTERVAL,
                    TimeValue.timeValueMillis(1L), Property.NodeScope);

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final String nodeName;
    private final Client client;
    private final QueueConsumer queueConsumer;
    private final ThreadPool threadPool;
    private final AtomicBoolean putTemplatePending = new AtomicBoolean(false);
    private final ClusterService clusterService;
    private final boolean indexToRemoteCluster;
    private final EnumSet<AuditLevel> events;
    private final IndexNameResolver.Rollover rollover;
    private final boolean includeRequestBody;

    private BulkProcessor bulkProcessor;
    private String nodeHostName;
    private String nodeHostAddress;

    @Override
    public String name() {
        return NAME;
    }

    public IndexAuditTrail(Settings settings, InternalClient client, ThreadPool threadPool, ClusterService clusterService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.nodeName = settings.get("name");
        final int maxQueueSize = QUEUE_SIZE_SETTING.get(settings);
        this.queueConsumer = new QueueConsumer(EsExecutors.threadName(settings, "audit-queue-consumer"), createQueue(maxQueueSize));
        this.rollover = ROLLOVER_SETTING.get(settings);
        this.events = parse(INCLUDE_EVENT_SETTINGS.get(settings), EXCLUDE_EVENT_SETTINGS.get(settings));
        this.indexToRemoteCluster = REMOTE_CLIENT_SETTINGS.get(settings).names().size() > 0;
        this.includeRequestBody = INCLUDE_REQUEST_BODY.get(settings);

        if (indexToRemoteCluster == false) {
            // in the absence of client settings for remote indexing, fall back to the client that was passed in.
            this.client = client;
        } else {
            this.client = initializeRemoteClient(settings, logger);
        }

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
     * <li>The current node must be the master OR the <code>security_audit_log</code> index template must exist</li>
     * <li>The current audit index must not exist or have all primary shards active. The current audit index name
     * is determined by the rollover settings and current time</li>
     * </ol>
     *
     * @param event  the {@link ClusterChangedEvent} containing the up to date cluster state
     * @param master flag indicating if the current node is the master
     * @return true if all requirements are met and the service can be started
     */
    public boolean canStart(ClusterChangedEvent event, boolean master) {
        if (indexToRemoteCluster) {
            // just return true as we do not determine whether we can start or not based on the local cluster state, but must base it off
            // of the remote cluster state and this method is called on the cluster state update thread, so we do not really want to
            // execute remote calls on this thread
            return true;
        }
        synchronized (this) {
            return canStart(event.state(), master);
        }
    }

    private boolean canStart(ClusterState clusterState, boolean master) {
        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have audit indices
            // but they may not have been restored from the cluster state on disk
            logger.debug("index audit trail waiting until gateway has recovered from disk");
            return false;
        }

        if (!master && clusterState.metaData().templates().get(INDEX_TEMPLATE_NAME) == null) {
            logger.debug("security audit index template [{}] does not exist, so service cannot start", INDEX_TEMPLATE_NAME);
            return false;
        }

        String index = getIndexName();
        IndexMetaData metaData = clusterState.metaData().index(index);
        if (metaData == null) {
            logger.debug("security audit index [{}] does not exist, so service can start", index);
            return true;
        }

        if (clusterState.routingTable().index(index).allPrimaryShardsActive()) {
            logger.debug("security audit index [{}] all primary shards started, so service can start", index);
            return true;
        }
        logger.debug("security audit index [{}] does not have all primary shards started, so service cannot start", index);
        return false;
    }

    private String getIndexName() {
        return resolve(INDEX_NAME_PREFIX, DateTime.now(DateTimeZone.UTC), rollover);
    }

    /**
     * Starts the service. The state is moved to {@link org.elasticsearch.xpack.security.audit.index.IndexAuditTrail.State#STARTING}
     * at the beginning of the method. The service's components are initialized and if the current node is the master, the index
     * template will be stored. The state is moved {@link org.elasticsearch.xpack.security.audit.index.IndexAuditTrail.State#STARTED}
     * and before returning the queue of messages that came before the service started is drained.
     *
     * @param master flag indicating if the current node is master
     */
    public void start(boolean master) {
        if (state.compareAndSet(State.INITIALIZED, State.STARTING)) {
            this.nodeHostName = clusterService.localNode().getHostName();
            this.nodeHostAddress = clusterService.localNode().getHostAddress();
            if (indexToRemoteCluster) {
                client.admin().cluster().prepareState().execute(new ActionListener<ClusterStateResponse>() {
                    @Override
                    public void onResponse(ClusterStateResponse clusterStateResponse) {
                        final boolean currentMaster = clusterService.state().getNodes().isLocalNodeElectedMaster();
                        if (canStart(clusterStateResponse.getState(), currentMaster)) {
                            if (currentMaster) {
                                putTemplate(customAuditIndexSettings(settings), ActionListener.wrap((v) -> innerStart(),
                                        (e) -> state.set(State.FAILED)));
                            } else {
                                innerStart();
                            }
                        } else {
                            if (state.compareAndSet(State.STARTING, State.INITIALIZED) == false) {
                                throw new IllegalStateException("state transition from starting to initialized failed, current value: " +
                                        state.get());
                            }
                            // for some reason we can't start up since the remote cluster is not fully setup. in this case
                            // we try to wait for yellow status (all primaries started up) this will also wait for
                            // state recovery etc.
                            String indexName = getIndexName();
                            // if this index doesn't exists the call will fail with a not_found exception...
                            client.admin().cluster().prepareHealth().setIndices().setWaitForYellowStatus().execute(ActionListener.wrap(
                                    (x) -> start(master),
                                    (e) -> logger.error("failed to get wait for yellow status on index [" + indexName + "]", e))
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("failed to get remote cluster state", e);
                    }
                });
            } else if (master) {
                putTemplate(customAuditIndexSettings(settings), ActionListener.wrap((v) -> innerStart(),
                        (e) -> state.set(State.FAILED)));
            } else {
                innerStart();
            }
        }
    }

    private void innerStart() {
        if (indexToRemoteCluster == false) {
            this.clusterService.addListener(this);
        }
        initializeBulkProcessor();
        queueConsumer.start();
        state.set(State.STARTED);
    }

    public synchronized void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            queueConsumer.close();
        }

        if (state() != State.STOPPED) {
            try {
                if (bulkProcessor != null) {
                    if (bulkProcessor.awaitClose(10, TimeUnit.SECONDS) == false) {
                        logger.warn("index audit trail failed to store all pending events after waiting for 10s");
                    }
                }
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
            } finally {
                if (indexToRemoteCluster) {
                    client.close();
                }
                state.set(State.STOPPED);
            }
        }
    }

    @Override
    public void authenticationSuccess(String realm, User user, RestRequest request) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            try {
                enqueue(message("authentication_success", realm, user, request), "authentication_success");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [authentication_success]", e);
            }
        }
    }

    @Override
    public void authenticationSuccess(String realm, User user, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            try {
                enqueue(message("authentication_success", action, user, realm, null, message), "authentication_success");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [authentication_success]", e);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            try {
                enqueue(message("anonymous_access_denied", action, (User) null, null, indices(message), message),
                        "anonymous_access_denied");
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
                enqueue(message("authentication_failed", action, (User) null, null, indices(message), message), "authentication_failed");
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
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            if (XPackUser.is(token.principal()) == false) {
                try {
                    enqueue(message("realm_authentication_failed", action, token, realm, indices(message), message),
                            "realm_authentication_failed");
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [authentication_failed]", e);
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            if (XPackUser.is(token.principal()) == false) {
                try {
                    enqueue(message("realm_authentication_failed", null, token, realm, null, request), "realm_authentication_failed");
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
                    enqueue(message("access_granted", action, user, null, indices(message), message), "access_granted");
                } catch (Exception e) {
                    logger.warn("failed to index audit event: [access_granted]", e);
                }
            }
        } else if (events.contains(ACCESS_GRANTED) && XPackUser.is(user) == false) {
            try {
                enqueue(message("access_granted", action, user, null, indices(message), message), "access_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [access_granted]", e);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message) {
        if (events.contains(ACCESS_DENIED) && XPackUser.is(user) == false) {
            try {
                enqueue(message("access_denied", action, user, null, indices(message), message), "access_denied");
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
                enqueue(message("tampered_request", action, (User) null, null, indices(message), message), "tampered_request");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [tampered_request]", e);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        if (events.contains(TAMPERED_REQUEST) && XPackUser.is(user) == false) {
            try {
                enqueue(message("tampered_request", action, user, null, indices(request), request), "tampered_request");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [tampered_request]", e);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED)) {
            try {
                enqueue(message("ip_filter", "connection_granted", inetAddress, profile, rule), "connection_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [connection_granted]", e);
            }
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
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
                enqueue(message("run_as_granted", action, user, null, null, message), "run_as_granted");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_granted]", e);
            }
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (events.contains(RUN_AS_DENIED)) {
            try {
                enqueue(message("run_as_denied", action, user, null, null, message), "run_as_denied");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_denied]", e);
            }
        }
    }

    @Override
    public void runAsDenied(User user, RestRequest request) {
        if (events.contains(RUN_AS_DENIED)) {
            try {
                enqueue(message("run_as_denied", null, user, request), "run_as_denied");
            } catch (Exception e) {
                logger.warn("failed to index audit event: [run_as_denied]", e);
            }
        }
    }

    private Message message(String type, @Nullable String action, @Nullable User user, @Nullable String realm,
                            @Nullable Set<String> indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder, clusterService.localNode(), threadPool.getThreadContext());

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
            msg.builder.array(Field.INDICES, indices.toArray(Strings.EMPTY_ARRAY));
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());

        return msg.end();
    }

    // FIXME - clean up the message generation
    private Message message(String type, @Nullable String action, @Nullable AuthenticationToken token,
                            @Nullable String realm, @Nullable Set<String> indices, TransportMessage message) throws Exception {

        Message msg = new Message().start();
        common("transport", type, msg.builder);
        originAttributes(message, msg.builder, clusterService.localNode(), threadPool.getThreadContext());

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
            msg.builder.array(Field.INDICES, indices.toArray(Strings.EMPTY_ARRAY));
        }
        msg.builder.field(Field.REQUEST, message.getClass().getSimpleName());

        return msg.end();
    }

    private Message message(String type, @Nullable String action, @Nullable AuthenticationToken token,
            @Nullable String realm, @Nullable Set<String> indices, RestRequest request) throws Exception {

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
            msg.builder.array(Field.INDICES, indices.toArray(Strings.EMPTY_ARRAY));
        }
        if (includeRequestBody) {
            msg.builder.field(Field.REQUEST_BODY, restRequestContent(request));
        }
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

    private Message message(String type, String realm, User user, RestRequest request) throws Exception {

        Message msg = new Message().start();
        common("rest", type, msg.builder);

        if (user != null) {
            if (user.runAs() != null) {
                msg.builder.field(Field.PRINCIPAL, user.runAs().principal());
                msg.builder.field(Field.RUN_BY_PRINCIPAL, user.principal());
            } else {
                msg.builder.field(Field.PRINCIPAL, user.principal());
            }
        }
        if (realm != null) {
            msg.builder.field(Field.REALM, realm);
        }
        if (includeRequestBody) {
            msg.builder.field(Field.REQUEST_BODY, restRequestContent(request));
        }
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
                            SecurityIpFilterRule rule) throws IOException {

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

    private static XContentBuilder originAttributes(TransportMessage message, XContentBuilder builder,
                                                    DiscoveryNode localNode, ThreadContext threadContext) throws IOException {

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
            builder.field(Field.ORIGIN_ADDRESS,
                    NetworkAddress.format(address.address().getAddress()));
            return builder;
        }

        // the call was originated locally on this node
        builder.field(Field.ORIGIN_TYPE, "local_node");
        builder.field(Field.ORIGIN_ADDRESS, localNode.getHostAddress());
        return builder;
    }

    void enqueue(Message message, String type) {
        State currentState = state();
        if (currentState != State.STOPPING && currentState != State.STOPPED) {
            boolean accepted = queueConsumer.offer(message);
            if (!accepted) {
                logger.warn("failed to index audit event: [{}]. internal queue is full, which may be caused by a high indexing rate or " +
                        "issue with the destination", type);
            }
        }
    }

    // for testing to ensure we get the proper timestamp and index name...
    Message peek() {
        return queueConsumer.peek();
    }

    Client initializeRemoteClient(Settings settings, Logger logger) {
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
                logger.warn("invalid host:port specified: [{}] for setting [{}.hosts]", REMOTE_CLIENT_SETTINGS.getKey(), host);
            }
            hostPortPairs.add(new Tuple<>(hostPort.get(0), hostPort.size() == 2 ? Integer.valueOf(hostPort.get(1)) : 9300));
        }

        if (hostPortPairs.size() == 0) {
            throw new ElasticsearchException("no valid host:port pairs specified for setting ["
                    + REMOTE_CLIENT_SETTINGS.getKey() + ".hosts]");
        }
        final Settings theClientSetting = clientSettings.filter((s) -> s.startsWith("hosts") == false); // hosts is not a valid setting
        final TransportClient transportClient = new TransportClient(Settings.builder()
                .put("node.name", DEFAULT_CLIENT_NAME + "-" + Node.NODE_NAME_SETTING.get(settings))
                .put(theClientSetting).build(), Settings.EMPTY, remoteTransportClientPlugins(), null) {};
        for (Tuple<String, Integer> pair : hostPortPairs) {
            try {
                transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(pair.v1()), pair.v2()));
            } catch (UnknownHostException e) {
                throw new ElasticsearchException("could not find host {}", e, pair.v1());
            }
        }

        logger.info("forwarding audit events to remote cluster [{}] using hosts [{}]",
                clientSettings.get("cluster.name", ""), hostPortPairs.toString());
        return transportClient;
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

    void putTemplate(Settings customSettings, ActionListener<Void> listener) {
        try (InputStream is = getClass().getResourceAsStream("/" + INDEX_TEMPLATE_NAME + ".json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            final byte[] template = out.toByteArray();
            final PutIndexTemplateRequest request = new PutIndexTemplateRequest(INDEX_TEMPLATE_NAME).source(template, XContentType.JSON);
            if (customSettings != null && customSettings.names().size() > 0) {
                Settings updatedSettings = Settings.builder()
                        .put(request.settings())
                        .put(customSettings)
                        .build();
                request.settings(updatedSettings);
            }

            client.admin().indices().putTemplate(request, ActionListener.wrap((response) -> {
                        if (response.isAcknowledged()) {
                            // now we may need to update the mappings of the current index
                            final DateTime dateTime;
                            final Message message = queueConsumer.peek();
                            if (message != null) {
                                dateTime = message.timestamp;
                            } else {
                                dateTime = DateTime.now(DateTimeZone.UTC);
                            }
                            final String index = resolve(INDEX_NAME_PREFIX, dateTime, rollover);
                            checkIfCurrentIndexExists(index, request, listener);
                        } else {
                            listener.onFailure(new IllegalStateException("failed to put index template for audit logging"));
                        }
                    }, listener::onFailure));
        } catch (Exception e) {
            logger.debug("unexpected exception while putting index template", e);
            listener.onFailure(e);
        }
    }

    private void checkIfCurrentIndexExists(String index, PutIndexTemplateRequest indexTemplateRequest, ActionListener<Void> listener) {
        client.admin().indices().prepareExists(index).execute(ActionListener.wrap((response) -> {
            if (response.isExists()) {
                logger.debug("index [{}] exists so we need to update mappings", index);
                putAuditIndexMappings(index, indexTemplateRequest, listener);
            } else {
                logger.debug("index [{}] does not exist so we do not need to update mappings", index);
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private void putAuditIndexMappings(String index, PutIndexTemplateRequest request, ActionListener<Void> listener) {
        String mappings = request.mappings().get(DOC_TYPE);
        client.admin().indices().preparePutMapping(index)
                .setType(DOC_TYPE)
                .setSource(mappings, XContentType.JSON)
                .execute(ActionListener.wrap((response) -> {
                    if (response.isAcknowledged()) {
                        listener.onResponse(null);
                    } else {
                        listener.onFailure(new IllegalStateException("failed to put mappings for audit logging index [" + index + "]"));
                    }
                },
                listener::onFailure));
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
                logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "failed to bulk index audit events: [{}]", failure.getMessage()), failure);
            }
        }).setBulkActions(bulkSize)
                .setFlushInterval(interval)
                .setConcurrentRequests(1)
                .build();
    }

    // this could be handled by a template registry service but adding that is extra complexity until we actually need it
    @Override
    public void clusterChanged(ClusterChangedEvent clusterChangedEvent) {
        assert indexToRemoteCluster == false;
        if (state() == State.STARTED
                && clusterChangedEvent.localNodeMaster()
                && clusterChangedEvent.state().metaData().templates().get(INDEX_TEMPLATE_NAME) == null
                && putTemplatePending.compareAndSet(false, true)) {
            logger.debug("security audit index template [{}] does not exist. it may have been deleted - putting the template",
                    INDEX_TEMPLATE_NAME);

            putTemplate(customAuditIndexSettings(settings), new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    putTemplatePending.set(false);
                }

                @Override
                public void onFailure(Exception e) {
                    putTemplatePending.set(false);
                    logger.error((Supplier<?>) () -> new ParameterizedMessage(
                            "failed to update security audit index template [{}]", INDEX_TEMPLATE_NAME), e);
                }
            });
        }
    }

    // method for testing to allow different plugins such as mock transport...
    List<Class<? extends Plugin>> remoteTransportClientPlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    public static void registerSettings(List<Setting<?>> settings) {
        settings.add(INDEX_SETTINGS);
        settings.add(EXCLUDE_EVENT_SETTINGS);
        settings.add(INCLUDE_EVENT_SETTINGS);
        settings.add(ROLLOVER_SETTING);
        settings.add(BULK_SIZE_SETTING);
        settings.add(FLUSH_TIMEOUT_SETTING);
        settings.add(QUEUE_SIZE_SETTING);
        settings.add(REMOTE_CLIENT_SETTINGS);
        settings.add(INCLUDE_REQUEST_BODY);
    }

    private final class QueueConsumer extends Thread implements Closeable {
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final BlockingQueue<Message> eventQueue;
        private final Message shutdownSentinelMessage;

        QueueConsumer(String name, BlockingQueue eventQueue) {
            super(name);
            this.eventQueue = eventQueue;
            try {
                shutdownSentinelMessage = new Message();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void close() {
            if (open.compareAndSet(true, false)) {
                try {
                    eventQueue.put(shutdownSentinelMessage);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void run() {
            while (open.get()) {
                try {
                    final Message message = eventQueue.take();
                    if (message == shutdownSentinelMessage || open.get() == false) {
                        break;
                    }
                    final IndexRequest indexRequest = client.prepareIndex()
                            .setIndex(resolve(INDEX_NAME_PREFIX, message.timestamp, rollover))
                            .setType(DOC_TYPE).setSource(message.builder).request();
                    bulkProcessor.add(indexRequest);
                } catch (InterruptedException e) {
                    logger.debug("index audit queue consumer interrupted", e);
                    close();
                    break;
                } catch (Exception e) {
                    // log the exception and keep going
                    logger.warn("failed to index audit message from queue", e);
                }
            }
            eventQueue.clear();
        }

        public boolean offer(Message message) {
            if (open.get()) {
                return eventQueue.offer(message);
            }
            return false;
        }

        public Message peek() {
            return eventQueue.peek();
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
