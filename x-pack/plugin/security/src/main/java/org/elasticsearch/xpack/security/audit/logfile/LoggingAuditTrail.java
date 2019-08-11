/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.StringMapMessage;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
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
import static org.elasticsearch.xpack.security.audit.AuditUtil.restRequestContent;

public class LoggingAuditTrail implements AuditTrail, ClusterStateListener {

    public static final String REST_ORIGIN_FIELD_VALUE = "rest";
    public static final String LOCAL_ORIGIN_FIELD_VALUE = "local_node";
    public static final String TRANSPORT_ORIGIN_FIELD_VALUE = "transport";
    public static final String IP_FILTER_ORIGIN_FIELD_VALUE = "ip_filter";

    // changing any of this names requires changing the log4j2.properties file too
    public static final String LOG_TYPE = "type";
    public static final String TIMESTAMP = "timestamp";
    public static final String ORIGIN_TYPE_FIELD_NAME = "origin.type";
    public static final String ORIGIN_ADDRESS_FIELD_NAME = "origin.address";
    public static final String NODE_NAME_FIELD_NAME = "node.name";
    public static final String NODE_ID_FIELD_NAME = "node.id";
    public static final String HOST_ADDRESS_FIELD_NAME = "host.ip";
    public static final String HOST_NAME_FIELD_NAME = "host.name";
    public static final String EVENT_TYPE_FIELD_NAME = "event.type";
    public static final String EVENT_ACTION_FIELD_NAME = "event.action";
    public static final String PRINCIPAL_FIELD_NAME = "user.name";
    public static final String PRINCIPAL_RUN_BY_FIELD_NAME = "user.run_by.name";
    public static final String PRINCIPAL_RUN_AS_FIELD_NAME = "user.run_as.name";
    public static final String PRINCIPAL_REALM_FIELD_NAME = "user.realm";
    public static final String PRINCIPAL_RUN_BY_REALM_FIELD_NAME = "user.run_by.realm";
    public static final String PRINCIPAL_RUN_AS_REALM_FIELD_NAME = "user.run_as.realm";
    public static final String PRINCIPAL_ROLES_FIELD_NAME = "user.roles";
    public static final String REALM_FIELD_NAME = "realm";
    public static final String URL_PATH_FIELD_NAME = "url.path";
    public static final String URL_QUERY_FIELD_NAME = "url.query";
    public static final String REQUEST_METHOD_FIELD_NAME = "request.method";
    public static final String REQUEST_BODY_FIELD_NAME = "request.body";
    public static final String REQUEST_ID_FIELD_NAME = "request.id";
    public static final String ACTION_FIELD_NAME = "action";
    public static final String INDICES_FIELD_NAME = "indices";
    public static final String REQUEST_NAME_FIELD_NAME = "request.name";
    public static final String TRANSPORT_PROFILE_FIELD_NAME = "transport.profile";
    public static final String RULE_FIELD_NAME = "rule";
    public static final String OPAQUE_ID_FIELD_NAME = "opaque_id";
    public static final String X_FORWARDED_FOR_FIELD_NAME = "x_forwarded_for";

    public static final String NAME = "logfile";
    public static final Setting<Boolean> EMIT_HOST_ADDRESS_SETTING = Setting.boolSetting(setting("audit.logfile.emit_node_host_address"),
            false, Property.NodeScope, Property.Dynamic);
    public static final Setting<Boolean> EMIT_HOST_NAME_SETTING = Setting.boolSetting(setting("audit.logfile.emit_node_host_name"),
            false, Property.NodeScope, Property.Dynamic);
    public static final Setting<Boolean> EMIT_NODE_NAME_SETTING = Setting.boolSetting(setting("audit.logfile.emit_node_name"),
            false, Property.NodeScope, Property.Dynamic);
    public static final Setting<Boolean> EMIT_NODE_ID_SETTING = Setting.boolSetting(setting("audit.logfile.emit_node_id"), true,
            Property.NodeScope, Property.Dynamic);
    private static final List<String> DEFAULT_EVENT_INCLUDES = Arrays.asList(ACCESS_DENIED.toString(), ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(), AUTHENTICATION_FAILED.toString(), CONNECTION_DENIED.toString(), TAMPERED_REQUEST.toString(),
            RUN_AS_DENIED.toString(), RUN_AS_GRANTED.toString());
    public static final Setting<List<String>> INCLUDE_EVENT_SETTINGS = Setting.listSetting(setting("audit.logfile.events.include"),
            DEFAULT_EVENT_INCLUDES, Function.identity(), Property.NodeScope, Property.Dynamic);
    public static final Setting<List<String>> EXCLUDE_EVENT_SETTINGS = Setting.listSetting(setting("audit.logfile.events.exclude"),
            Collections.emptyList(), Function.identity(), Property.NodeScope, Property.Dynamic);
    public static final Setting<Boolean> INCLUDE_REQUEST_BODY = Setting.boolSetting(setting("audit.logfile.events.emit_request_body"),
            false, Property.NodeScope, Property.Dynamic);
    private static final String FILTER_POLICY_PREFIX = setting("audit.logfile.events.ignore_filters.");
    // because of the default wildcard value (*) for the field filter, a policy with
    // an unspecified filter field will match events that have any value for that
    // particular field, as well as events with that particular field missing
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_PRINCIPALS = Setting.affixKeySetting(FILTER_POLICY_PREFIX,
            "users",
            (key) -> Setting.listSetting(key, Collections.singletonList("*"), Function.identity(), Property.NodeScope, Property.Dynamic));
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_REALMS = Setting.affixKeySetting(FILTER_POLICY_PREFIX,
            "realms",
            (key) -> Setting.listSetting(key, Collections.singletonList("*"), Function.identity(), Property.NodeScope, Property.Dynamic));
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_ROLES = Setting.affixKeySetting(FILTER_POLICY_PREFIX,
            "roles",
            (key) -> Setting.listSetting(key, Collections.singletonList("*"), Function.identity(), Property.NodeScope, Property.Dynamic));
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_INDICES = Setting.affixKeySetting(FILTER_POLICY_PREFIX,
            "indices",
            (key) -> Setting.listSetting(key, Collections.singletonList("*"), Function.identity(), Property.NodeScope, Property.Dynamic));

    private final Logger logger;
    private final ThreadContext threadContext;
    final EventFilterPolicyRegistry eventFilterPolicyRegistry;
    // package for testing
    volatile EnumSet<AuditLevel> events;
    boolean includeRequestBody;
    // fields that all entries have in common
    EntryCommonFields entryCommonFields;

    @Override
    public String name() {
        return NAME;
    }

    public LoggingAuditTrail(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        this(settings, clusterService, LogManager.getLogger(), threadPool.getThreadContext());
    }

    LoggingAuditTrail(Settings settings, ClusterService clusterService, Logger logger, ThreadContext threadContext) {
        this.logger = logger;
        this.events = parse(INCLUDE_EVENT_SETTINGS.get(settings), EXCLUDE_EVENT_SETTINGS.get(settings));
        this.includeRequestBody = INCLUDE_REQUEST_BODY.get(settings);
        this.threadContext = threadContext;
        this.entryCommonFields = new EntryCommonFields(settings, null);
        this.eventFilterPolicyRegistry = new EventFilterPolicyRegistry(settings);
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(newSettings -> {
            this.entryCommonFields = this.entryCommonFields.withNewSettings(newSettings);
            this.includeRequestBody = INCLUDE_REQUEST_BODY.get(newSettings);
            // `events` is a volatile field! Keep `events` write last so that
            // `entryCommonFields` and `includeRequestBody` writes happen-before! `events` is
            // always read before `entryCommonFields` and `includeRequestBody`.
            this.events = parse(INCLUDE_EVENT_SETTINGS.get(newSettings), EXCLUDE_EVENT_SETTINGS.get(newSettings));
        }, Arrays.asList(EMIT_HOST_ADDRESS_SETTING, EMIT_HOST_NAME_SETTING, EMIT_NODE_NAME_SETTING, EMIT_NODE_ID_SETTING,
                INCLUDE_EVENT_SETTINGS, EXCLUDE_EVENT_SETTINGS, INCLUDE_REQUEST_BODY));
        clusterService.getClusterSettings().addAffixUpdateConsumer(FILTER_POLICY_IGNORE_PRINCIPALS, (policyName, filtersList) -> {
            final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
            final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings))
                    .changePrincipalsFilter(filtersList);
            this.eventFilterPolicyRegistry.set(policyName, newPolicy);
        }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
        clusterService.getClusterSettings().addAffixUpdateConsumer(FILTER_POLICY_IGNORE_REALMS, (policyName, filtersList) -> {
            final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
            final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings)).changeRealmsFilter(filtersList);
            this.eventFilterPolicyRegistry.set(policyName, newPolicy);
        }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
        clusterService.getClusterSettings().addAffixUpdateConsumer(FILTER_POLICY_IGNORE_ROLES, (policyName, filtersList) -> {
            final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
            final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings)).changeRolesFilter(filtersList);
            this.eventFilterPolicyRegistry.set(policyName, newPolicy);
        }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
        clusterService.getClusterSettings().addAffixUpdateConsumer(FILTER_POLICY_IGNORE_INDICES, (policyName, filtersList) -> {
            final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
            final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings)).changeIndicesFilter(filtersList);
            this.eventFilterPolicyRegistry.set(policyName, newPolicy);
        }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
    }

    @Override
    public void authenticationSuccess(String requestId, String realm, User user, RestRequest request) {
        if (events.contains(AUTHENTICATION_SUCCESS) && eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(realm), Optional.empty(), Optional.empty())) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "authentication_success")
                    .with(REALM_FIELD_NAME, realm)
                    .withRestUriAndMethod(request)
                    .withRequestId(requestId)
                    .withPrincipal(user)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void authenticationSuccess(String requestId, String realm, User user, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(realm), Optional.empty(), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "authentication_success")
                        .with(REALM_FIELD_NAME, realm)
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withPrincipal(user)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, String action, TransportMessage message) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, RestRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)
                && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
                    .withRestUriAndMethod(request)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withRequestId(requestId)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                        .with(ACTION_FIELD_NAME, action)
                        .with(PRINCIPAL_FIELD_NAME, token.principal())
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                    .withRestUriAndMethod(request)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withRequestId(requestId)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void authenticationFailed(String requestId, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED) && eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), Optional.empty())) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                    .with(PRINCIPAL_FIELD_NAME, token.principal())
                    .withRestUriAndMethod(request)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withRequestId(requestId)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                        .with(REALM_FIELD_NAME, realm)
                        .with(PRINCIPAL_FIELD_NAME, token.principal())
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {
        if (events.contains(REALM_AUTHENTICATION_FAILED) && eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), Optional.empty())) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                    .with(REALM_FIELD_NAME, realm)
                    .with(PRINCIPAL_FIELD_NAME, token.principal())
                    .withRestUriAndMethod(request)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withRequestId(requestId)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void accessGranted(String requestId, Authentication authentication, String action, TransportMessage msg,
                              AuthorizationInfo authorizationInfo) {
        final User user = authentication.getUser();
        final boolean isSystem = SystemUser.is(user) || XPackUser.is(user);
        if ((isSystem && events.contains(SYSTEM_ACCESS_GRANTED)) || ((isSystem == false) && events.contains(ACCESS_GRANTED))) {
            final Optional<String[]> indices = indices(msg);
            if (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(user),
                    Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "access_granted")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, msg.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withSubject(authentication)
                        .withRestOrTransportOrigin(msg, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .with(authorizationInfo.asMap())
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void explicitIndexAccessEvent(String requestId, AuditLevel eventType, Authentication authentication, String action, String index,
                                    String requestName, TransportAddress remoteAddress, AuthorizationInfo authorizationInfo) {
        assert eventType == ACCESS_DENIED || eventType == AuditLevel.ACCESS_GRANTED || eventType == SYSTEM_ACCESS_GRANTED;
        final String[] indices = index == null ? null : new String[] { index };
        final User user = authentication.getUser();
        final boolean isSystem = SystemUser.is(user) || XPackUser.is(user);
        if (isSystem && eventType == ACCESS_GRANTED) {
            eventType = SYSTEM_ACCESS_GRANTED;
        }
        if (events.contains(eventType)) {
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(effectiveRealmName(authentication)),
                            Optional.of(authorizationInfo), Optional.ofNullable(indices))) == false) {
                final LogEntryBuilder logEntryBuilder = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, eventType == ACCESS_DENIED ? "access_denied" : "access_granted")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, requestName)
                        .withRequestId(requestId)
                        .withSubject(authentication)
                        .with(INDICES_FIELD_NAME, indices)
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .with(authorizationInfo.asMap());
                final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
                if (restAddress != null) {
                    logEntryBuilder
                        .with(ORIGIN_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                        .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
                } else if (remoteAddress != null) {
                    logEntryBuilder
                        .with(ORIGIN_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(remoteAddress.address()));
                }
                logger.info(logEntryBuilder.build());
            }
        }
    }

    @Override
    public void accessDenied(String requestId, Authentication authentication, String action, TransportMessage message,
                             AuthorizationInfo authorizationInfo) {
        if (events.contains(ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(authentication.getUser()),
                    Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "access_denied")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withSubject(authentication)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .with(authorizationInfo.asMap())
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, RestRequest request) {
        if (events.contains(TAMPERED_REQUEST) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "tampered_request")
                    .withRestUriAndMethod(request)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withRequestId(requestId)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void tamperedRequest(String requestId, String action, TransportMessage message) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "tampered_request")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, User user, String action, TransportMessage message) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.empty(), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "tampered_request")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRestOrTransportOrigin(message, threadContext)
                        .withPrincipal(user)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, IP_FILTER_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "connection_granted")
                    .with(ORIGIN_TYPE_FIELD_NAME,
                            IPFilter.HTTP_PROFILE_NAME.equals(profile) ? REST_ORIGIN_FIELD_VALUE : TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
                    .with(TRANSPORT_PROFILE_FIELD_NAME, profile)
                    .with(RULE_FIELD_NAME, rule.toString())
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, IP_FILTER_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "connection_denied")
                    .with(ORIGIN_TYPE_FIELD_NAME,
                            IPFilter.HTTP_PROFILE_NAME.equals(profile) ? REST_ORIGIN_FIELD_VALUE : TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
                    .with(TRANSPORT_PROFILE_FIELD_NAME, profile)
                    .with(RULE_FIELD_NAME, rule.toString())
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    @Override
    public void runAsGranted(String requestId, Authentication authentication, String action, TransportMessage message,
                             AuthorizationInfo authorizationInfo) {
        if (events.contains(RUN_AS_GRANTED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(authentication.getUser()),
                    Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "run_as_granted")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRunAsSubject(authentication)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .with(authorizationInfo.asMap())
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, String action, TransportMessage message,
                            AuthorizationInfo authorizationInfo) {
        if (events.contains(RUN_AS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(authentication.getUser()),
                    Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), indices)) == false) {
                final StringMapMessage logEntry = new LogEntryBuilder()
                        .with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(EVENT_ACTION_FIELD_NAME, "run_as_denied")
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                        .withRequestId(requestId)
                        .withRunAsSubject(authentication)
                        .withRestOrTransportOrigin(message, threadContext)
                        .with(INDICES_FIELD_NAME, indices.orElse(null))
                        .with(authorizationInfo.asMap())
                        .withOpaqueId(threadContext)
                        .withXForwardedFor(threadContext)
                        .build();
                logger.info(logEntry);
            }
        }
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, RestRequest request, AuthorizationInfo authorizationInfo) {
        if (events.contains(RUN_AS_DENIED)
                && eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(authentication.getUser()),
                        Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), Optional.empty())) == false) {
            final StringMapMessage logEntry = new LogEntryBuilder()
                    .with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "run_as_denied")
                    .with(authorizationInfo.asMap())
                    .withRestUriAndMethod(request)
                    .withRunAsSubject(authentication)
                    .withRestOrigin(request)
                    .withRequestBody(request)
                    .withRequestId(requestId)
                    .withOpaqueId(threadContext)
                    .withXForwardedFor(threadContext)
                    .build();
            logger.info(logEntry);
        }
    }

    private class LogEntryBuilder {

        private final StringMapMessage logEntry;

        LogEntryBuilder() {
            logEntry = new StringMapMessage(LoggingAuditTrail.this.entryCommonFields.commonFields);
        }

        LogEntryBuilder withRestUriAndMethod(RestRequest request) {
            final int queryStringIndex = request.uri().indexOf('?');
            int queryStringLength = request.uri().indexOf('#');
            if (queryStringLength < 0) {
                queryStringLength = request.uri().length();
            }
            if (queryStringIndex < 0) {
                logEntry.with(URL_PATH_FIELD_NAME, request.uri().substring(0, queryStringLength));
            } else {
                logEntry.with(URL_PATH_FIELD_NAME, request.uri().substring(0, queryStringIndex));
            }
            if (queryStringIndex > -1) {
                logEntry.with(URL_QUERY_FIELD_NAME, request.uri().substring(queryStringIndex + 1, queryStringLength));
            }
            logEntry.with(REQUEST_METHOD_FIELD_NAME, request.method().toString());
            return this;
        }

        LogEntryBuilder withRunAsSubject(Authentication authentication) {
            logEntry.with(PRINCIPAL_FIELD_NAME, authentication.getUser().authenticatedUser().principal())
                    .with(PRINCIPAL_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName())
                    .with(PRINCIPAL_RUN_AS_FIELD_NAME, authentication.getUser().principal());
            if (authentication.getLookedUpBy() != null) {
                logEntry.with(PRINCIPAL_RUN_AS_REALM_FIELD_NAME, authentication.getLookedUpBy().getName());
            }
            return this;
        }

        LogEntryBuilder withRestOrigin(RestRequest request) {
            assert LOCAL_ORIGIN_FIELD_VALUE.equals(logEntry.get(ORIGIN_TYPE_FIELD_NAME)); // this is the default
            final InetSocketAddress socketAddress = request.getHttpChannel().getRemoteAddress();
            if (socketAddress != null) {
                logEntry.with(ORIGIN_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                        .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(socketAddress));
            }
            // fall through to local_node default
            return this;
        }

        LogEntryBuilder withRestOrTransportOrigin(TransportMessage message, ThreadContext threadContext) {
            assert LOCAL_ORIGIN_FIELD_VALUE.equals(logEntry.get(ORIGIN_TYPE_FIELD_NAME)); // this is the default
            final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
            if (restAddress != null) {
                logEntry.with(ORIGIN_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                        .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
            } else {
                final TransportAddress address = message.remoteAddress();
                if (address != null) {
                    logEntry.with(ORIGIN_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                            .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address.address()));
                }
            }
            // fall through to local_node default
            return this;
        }

        LogEntryBuilder withRequestBody(RestRequest request) {
            if (includeRequestBody) {
                final String requestContent = restRequestContent(request);
                if (Strings.hasLength(requestContent)) {
                    logEntry.with(REQUEST_BODY_FIELD_NAME, requestContent);
                }
            }
            return this;
        }

        LogEntryBuilder withRequestId(String requestId) {
            if (requestId != null) {
                logEntry.with(REQUEST_ID_FIELD_NAME, requestId);
            }
            return this;
        }

        LogEntryBuilder withOpaqueId(ThreadContext threadContext) {
            final String opaqueId = threadContext.getHeader(Task.X_OPAQUE_ID);
            if (opaqueId != null) {
                logEntry.with(OPAQUE_ID_FIELD_NAME, opaqueId);
            }
            return this;
        }

        LogEntryBuilder withXForwardedFor(ThreadContext threadContext) {
            final String xForwardedFor = threadContext.getHeader(AuditTrail.X_FORWARDED_FOR_HEADER);
            if (xForwardedFor != null) {
                logEntry.with(X_FORWARDED_FOR_FIELD_NAME, xForwardedFor);
            }
            return this;
        }

        LogEntryBuilder withPrincipal(User user) {
            logEntry.with(PRINCIPAL_FIELD_NAME, user.principal());
            if (user.isRunAs()) {
                logEntry.with(PRINCIPAL_RUN_BY_FIELD_NAME, user.authenticatedUser().principal());
            }
            return this;
        }

        LogEntryBuilder withSubject(Authentication authentication) {
            logEntry.with(PRINCIPAL_FIELD_NAME, authentication.getUser().principal());
            if (authentication.getUser().isRunAs()) {
                logEntry.with(PRINCIPAL_REALM_FIELD_NAME, authentication.getLookedUpBy().getName())
                        .with(PRINCIPAL_RUN_BY_FIELD_NAME, authentication.getUser().authenticatedUser().principal())
                        .with(PRINCIPAL_RUN_BY_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
            } else {
                logEntry.with(PRINCIPAL_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
            }
            return this;
        }

        LogEntryBuilder with(String key, String value) {
            if (value != null) {
                logEntry.with(key, value);
            }
            return this;
        }

        LogEntryBuilder with(String key, String[] values) {
            if (values != null) {
                logEntry.with(key, toQuotedJsonArray(values));
            }
            return this;
        }

        LogEntryBuilder with(Map<String, Object> map) {
            for (Entry<String, Object> entry : map.entrySet()) {
                Object value = entry.getValue();
                if (value.getClass().isArray()) {
                    logEntry.with(entry.getKey(), toQuotedJsonArray((Object[]) value));
                } else {
                    logEntry.with(entry.getKey(), value);
                }
            }
            return this;
        }

        StringMapMessage build() {
            return logEntry;
        }

        String toQuotedJsonArray(Object[] values) {
            assert values != null;
            final StringBuilder stringBuilder = new StringBuilder();
            final JsonStringEncoder jsonStringEncoder = JsonStringEncoder.getInstance();
            stringBuilder.append("[");
            for (final Object value : values) {
                if (value != null) {
                    if (stringBuilder.length() > 1) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"");
                    jsonStringEncoder.quoteAsString(value.toString(), stringBuilder);
                    stringBuilder.append("\"");
                }
            }
            stringBuilder.append("]");
            return stringBuilder.toString();
        }
    }


    private static Optional<String[]> indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            final String[] indices = ((IndicesRequest) message).indices();
            if (indices != null) {
                return Optional.of(((IndicesRequest) message).indices());
            }
        }
        return Optional.empty();
    }

    private static String effectiveRealmName(Authentication authentication) {
        return authentication.getLookedUpBy() != null ? authentication.getLookedUpBy().getName()
                : authentication.getAuthenticatedBy().getName();
    }

    public static void registerSettings(List<Setting<?>> settings) {
        settings.add(EMIT_HOST_ADDRESS_SETTING);
        settings.add(EMIT_HOST_NAME_SETTING);
        settings.add(EMIT_NODE_NAME_SETTING);
        settings.add(EMIT_NODE_ID_SETTING);
        settings.add(INCLUDE_EVENT_SETTINGS);
        settings.add(EXCLUDE_EVENT_SETTINGS);
        settings.add(INCLUDE_REQUEST_BODY);
        settings.add(FILTER_POLICY_IGNORE_PRINCIPALS);
        settings.add(FILTER_POLICY_IGNORE_INDICES);
        settings.add(FILTER_POLICY_IGNORE_ROLES);
        settings.add(FILTER_POLICY_IGNORE_REALMS);
    }

    /**
     * Builds the predicate for a single policy filter. The predicate matches events
     * that will be ignored, aka filtered out, aka not logged. The event can be
     * filtered by the following fields : `user`, `realm`, `role` and `index`.
     * Predicates on each field are ANDed together to form the filter predicate of
     * the policy.
     */
    private static final class EventFilterPolicy {
        private final String name;
        private final Predicate<String> ignorePrincipalsPredicate;
        private final Predicate<String> ignoreRealmsPredicate;
        private final Predicate<String> ignoreRolesPredicate;
        private final Predicate<String> ignoreIndicesPredicate;

        EventFilterPolicy(String name, Settings settings) {
            this(name, parsePredicate(FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace(name).get(settings)),
                    parsePredicate(FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace(name).get(settings)),
                    parsePredicate(FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace(name).get(settings)),
                    parsePredicate(FILTER_POLICY_IGNORE_INDICES.getConcreteSettingForNamespace(name).get(settings)));
        }

        /**
         * An empty filter list for a field will match events with that field missing.
         * An event with an undefined field has the field value the empty string ("") or
         * a singleton list of the empty string ([""]).
         */
        EventFilterPolicy(String name, Predicate<String> ignorePrincipalsPredicate, Predicate<String> ignoreRealmsPredicate,
                Predicate<String> ignoreRolesPredicate, Predicate<String> ignoreIndicesPredicate) {
            this.name = name;
            // "null" values are "unexpected" and should not match any ignore policy
            this.ignorePrincipalsPredicate = ignorePrincipalsPredicate;
            this.ignoreRealmsPredicate = ignoreRealmsPredicate;
            this.ignoreRolesPredicate = ignoreRolesPredicate;
            this.ignoreIndicesPredicate = ignoreIndicesPredicate;
        }

        private EventFilterPolicy changePrincipalsFilter(List<String> filtersList) {
            return new EventFilterPolicy(name, parsePredicate(filtersList), ignoreRealmsPredicate, ignoreRolesPredicate,
                    ignoreIndicesPredicate);
        }

        private EventFilterPolicy changeRealmsFilter(List<String> filtersList) {
            return new EventFilterPolicy(name, ignorePrincipalsPredicate, parsePredicate(filtersList), ignoreRolesPredicate,
                    ignoreIndicesPredicate);
        }

        private EventFilterPolicy changeRolesFilter(List<String> filtersList) {
            return new EventFilterPolicy(name, ignorePrincipalsPredicate, ignoreRealmsPredicate, parsePredicate(filtersList),
                    ignoreIndicesPredicate);
        }

        private EventFilterPolicy changeIndicesFilter(List<String> filtersList) {
            return new EventFilterPolicy(name, ignorePrincipalsPredicate, ignoreRealmsPredicate, ignoreRolesPredicate,
                    parsePredicate(filtersList));
        }

        static Predicate<String> parsePredicate(List<String> l) {
            return Automatons.predicate(emptyStringBuildsEmptyAutomaton(l));
        }

        /**
         * It is a requirement that empty string filters match empty string fields. In
         * this case we require automatons from empty string to match the empty string.
         * `Automatons.predicate("").test("") == false`
         * `Automatons.predicate("//").test("") == true`
         */
        private static List<String> emptyStringBuildsEmptyAutomaton(List<String> l) {
            if (l.isEmpty()) {
                return Collections.singletonList("//");
            }
            return l.stream().map(f -> f.isEmpty() ? "//" : f).collect(Collectors.toList());
        }

        /**
         * ANDs the predicates of this filter policy. The `indices` and `roles` fields
         * of an audit event are multi-valued and all values should match the filter
         * predicate of the corresponding field.
         */
        Predicate<AuditEventMetaInfo> ignorePredicate() {
            return eventInfo -> eventInfo.principal != null && ignorePrincipalsPredicate.test(eventInfo.principal)
                    && eventInfo.realm != null && ignoreRealmsPredicate.test(eventInfo.realm)
                    && eventInfo.roles.get().allMatch(role -> role != null && ignoreRolesPredicate.test(role))
                    && eventInfo.indices.get().allMatch(index -> index != null && ignoreIndicesPredicate.test(index));
        }

        @Override
        public String toString() {
            return "[users]:" + ignorePrincipalsPredicate.toString() + "&[realms]:" + ignoreRealmsPredicate.toString() + "&[roles]:"
                    + ignoreRolesPredicate.toString() + "&[indices]:" + ignoreIndicesPredicate.toString();
        }
    }

    /**
     * Builds the filter predicates for all the policies. Predicates of all policies
     * are ORed together, so that an audit event matching any policy is ignored.
     */
    static final class EventFilterPolicyRegistry {
        private volatile Map<String, EventFilterPolicy> policyMap;
        private volatile Predicate<AuditEventMetaInfo> predicate;

        private EventFilterPolicyRegistry(Settings settings) {
            final var entries = new ArrayList<Map.Entry<String, EventFilterPolicy>>();
            for (final String policyName : settings.getGroups(FILTER_POLICY_PREFIX, true).keySet()) {
                entries.add(entry(policyName, new EventFilterPolicy(policyName, settings)));
            }
            policyMap = Maps.ofEntries(entries);
            // precompute predicate
            predicate = buildIgnorePredicate(policyMap);
        }

        private Optional<EventFilterPolicy> get(String policyName) {
            return Optional.ofNullable(policyMap.get(policyName));
        }

        private synchronized void set(String policyName, EventFilterPolicy eventFilterPolicy) {
            policyMap = Maps.copyMayWithAddedOrReplacedEntry(policyMap, policyName, eventFilterPolicy);
            // precompute predicate
            predicate = buildIgnorePredicate(policyMap);
        }

        Predicate<AuditEventMetaInfo> ignorePredicate() {
            return predicate;
        }

        private static Predicate<AuditEventMetaInfo> buildIgnorePredicate(Map<String, EventFilterPolicy> policyMap) {
            return policyMap.values().stream().map(EventFilterPolicy::ignorePredicate).reduce(x -> false, (x, y) -> x.or(y));
        }

        @Override
        public String toString() {
            final Map<String, EventFilterPolicy> treeMap = new TreeMap<>(policyMap);
            final StringBuilder sb = new StringBuilder();
            for (final Map.Entry<String, EventFilterPolicy> entry : treeMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue().toString());
            }
            return sb.toString();
        }
    }

    /**
     * Abstraction for the fields of the audit event that are used for filtering. If
     * an event has a missing field (one of `user`, `realm`, `roles` and `indices`)
     * the value for the field will be the empty string or a singleton stream of the
     * empty string.
     */
    static final class AuditEventMetaInfo {
        final String principal;
        final String realm;
        final Supplier<Stream<String>> roles;
        final Supplier<Stream<String>> indices;

        // empty is used for events can be filtered out only by the lack of a field
        static final AuditEventMetaInfo EMPTY = new AuditEventMetaInfo(Optional.empty(), Optional.empty(), Optional.empty());

        /**
         * If a field is missing for an event, its value for filtering purposes is the
         * empty string or a singleton stream of the empty string. This a allows a
         * policy to filter by the missing value using the empty string, ie
         * `ignore_filters.users: ["", "elastic"]` will filter events with a missing
         * user field (such as `anonymous_access_denied`) as well as events from the
         * "elastic" username.
         */
        AuditEventMetaInfo(Optional<User> user, Optional<String> realm, Optional<AuthorizationInfo> authorizationInfo,
                           Optional<String[]> indices) {
            this.principal = user.map(u -> u.principal()).orElse("");
            this.realm = realm.orElse("");
            // Supplier indirection and lazy generation of Streams serves 2 purposes:
            // 1. streams might not get generated due to short circuiting logical
            // conditions on the `principal` and `realm` fields
            // 2. reusability of the AuditEventMetaInfo instance: in this case Streams have
            // to be regenerated as they cannot be operated upon twice
            this.roles = () -> authorizationInfo.filter(info -> {
                final Object value = info.asMap().get("user.roles");
                return value instanceof String[] &&
                    ((String[]) value).length != 0 &&
                    Arrays.stream((String[]) value).anyMatch(Objects::nonNull);
            }).map(info -> Arrays.stream((String[]) info.asMap().get("user.roles"))).orElse(Stream.of(""));
            this.indices = () -> indices.filter(i -> i.length > 0).filter(a -> Arrays.stream(a).anyMatch(Objects::nonNull))
                    .map(Arrays::stream).orElse(Stream.of(""));
        }

        AuditEventMetaInfo(Optional<AuthenticationToken> authenticationToken, Optional<String> realm, Optional<String[]> indices) {
            this.principal = authenticationToken.map(u -> u.principal()).orElse("");
            this.realm = realm.orElse("");
            this.roles = () -> Stream.of("");
            this.indices = () -> indices.filter(r -> r.length != 0).map(i -> Arrays.stream(i)).orElse(Stream.of(""));
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        updateLocalNodeInfo(event.state().getNodes().getLocalNode());
    }

    void updateLocalNodeInfo(DiscoveryNode newLocalNode) {
        // check if local node changed
        final EntryCommonFields localNodeInfo = this.entryCommonFields;
        if (localNodeInfo.localNode == null || localNodeInfo.localNode.equals(newLocalNode) == false) {
            // no need to synchronize, called only from the cluster state applier thread
            this.entryCommonFields = this.entryCommonFields.withNewLocalNode(newLocalNode);
        }
    }

    static class EntryCommonFields {
        private final Settings settings;
        private final DiscoveryNode localNode;
        final Map<String, String> commonFields;

        EntryCommonFields(Settings settings, @Nullable DiscoveryNode newLocalNode) {
            this.settings = settings;
            this.localNode = newLocalNode;
            final Map<String, String> commonFields = new HashMap<>();
            if (EMIT_NODE_NAME_SETTING.get(settings)) {
                final String nodeName = Node.NODE_NAME_SETTING.get(settings);
                if (Strings.hasLength(nodeName)) {
                    commonFields.put(NODE_NAME_FIELD_NAME, nodeName);
                }
            }
            if (newLocalNode != null && newLocalNode.getAddress() != null) {
                if (EMIT_HOST_ADDRESS_SETTING.get(settings)) {
                    commonFields.put(HOST_ADDRESS_FIELD_NAME, newLocalNode.getAddress().getAddress());
                }
                if (EMIT_HOST_NAME_SETTING.get(settings)) {
                    commonFields.put(HOST_NAME_FIELD_NAME, newLocalNode.getAddress().address().getHostString());
                }
                if (EMIT_NODE_ID_SETTING.get(settings)) {
                    commonFields.put(NODE_ID_FIELD_NAME, newLocalNode.getId());
                }
                // the default origin is local
                commonFields.put(ORIGIN_ADDRESS_FIELD_NAME, newLocalNode.getAddress().toString());
            }
            // the default origin is local
            commonFields.put(ORIGIN_TYPE_FIELD_NAME, LOCAL_ORIGIN_FIELD_VALUE);
            this.commonFields = Collections.unmodifiableMap(commonFields);
        }

        EntryCommonFields withNewSettings(Settings newSettings) {
            final Settings mergedSettings = Settings.builder().put(this.settings).put(newSettings, false).build();
            return new EntryCommonFields(mergedSettings, this.localNode);
        }

        EntryCommonFields withNewLocalNode(DiscoveryNode newLocalNode) {
            return new EntryCommonFields(this.settings, newLocalNode);
        }
    }
}
