/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.Filter.Result;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.filter.MarkerFilter;
import org.apache.logging.log4j.message.StringMapMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_NAME_FIELD;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_SOURCE_FIELD;
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
import static org.elasticsearch.xpack.security.audit.AuditLevel.SECURITY_CONFIG_CHANGE;
import static org.elasticsearch.xpack.security.audit.AuditLevel.SYSTEM_ACCESS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.TAMPERED_REQUEST;
import static org.elasticsearch.xpack.security.audit.AuditLevel.parse;
import static org.elasticsearch.xpack.security.audit.AuditUtil.restRequestContent;

public class LoggingAuditTrail implements AuditTrail, ClusterStateListener {

    public static final String REST_ORIGIN_FIELD_VALUE = "rest";
    public static final String LOCAL_ORIGIN_FIELD_VALUE = "local_node";
    public static final String TRANSPORT_ORIGIN_FIELD_VALUE = "transport";
    public static final String IP_FILTER_ORIGIN_FIELD_VALUE = "ip_filter";
    public static final String SECURITY_CHANGE_ORIGIN_FIELD_VALUE = "security_config_change";

    // changing any of these field names requires changing the log4j2.properties file(s) too
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
    public static final String API_KEY_ID_FIELD_NAME = "apikey.id";
    public static final String API_KEY_NAME_FIELD_NAME = "apikey.name";
    public static final String SERVICE_TOKEN_NAME_FIELD_NAME = "authentication.token.name";
    public static final String SERVICE_TOKEN_TYPE_FIELD_NAME = "authentication.token.type";
    public static final String PRINCIPAL_ROLES_FIELD_NAME = "user.roles";
    public static final String AUTHENTICATION_TYPE_FIELD_NAME = "authentication.type";
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
    public static final String TRACE_ID_FIELD_NAME = "trace.id";
    public static final String X_FORWARDED_FOR_FIELD_NAME = "x_forwarded_for";
    // the fields below are used exclusively for "security_config_change" type of events, and show the configuration
    // object taking effect; it could be creating a new, or updating an existing configuration
    // if our (REST) APIs (at least the security APIs) would make the distinction between creating a *new* resource using the POST
    // verb and updating an *existing* resource using the PUT verb, then auditing would also be able to show the create/update distinction
    public static final String PUT_CONFIG_FIELD_NAME = "put";
    public static final String DELETE_CONFIG_FIELD_NAME = "delete";
    public static final String CHANGE_CONFIG_FIELD_NAME = "change";
    public static final String CREATE_CONFIG_FIELD_NAME = "create";
    public static final String INVALIDATE_API_KEYS_FIELD_NAME = "invalidate";

    public static final String NAME = "logfile";
    public static final Setting<Boolean> EMIT_HOST_ADDRESS_SETTING = Setting.boolSetting(
        setting("audit.logfile.emit_node_host_address"),
        false,
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<Boolean> EMIT_HOST_NAME_SETTING = Setting.boolSetting(
        setting("audit.logfile.emit_node_host_name"),
        false,
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<Boolean> EMIT_NODE_NAME_SETTING = Setting.boolSetting(
        setting("audit.logfile.emit_node_name"),
        false,
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<Boolean> EMIT_NODE_ID_SETTING = Setting.boolSetting(
        setting("audit.logfile.emit_node_id"),
        true,
        Property.NodeScope,
        Property.Dynamic
    );
    private static final List<String> DEFAULT_EVENT_INCLUDES = Arrays.asList(
        ACCESS_DENIED.toString(),
        ACCESS_GRANTED.toString(),
        ANONYMOUS_ACCESS_DENIED.toString(),
        AUTHENTICATION_FAILED.toString(),
        CONNECTION_DENIED.toString(),
        TAMPERED_REQUEST.toString(),
        RUN_AS_DENIED.toString(),
        RUN_AS_GRANTED.toString(),
        SECURITY_CONFIG_CHANGE.toString()
    );
    public static final Setting<List<String>> INCLUDE_EVENT_SETTINGS = Setting.listSetting(
        setting("audit.logfile.events.include"),
        DEFAULT_EVENT_INCLUDES,
        Function.identity(),
        value -> AuditLevel.parse(value, Collections.emptyList()),
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<List<String>> EXCLUDE_EVENT_SETTINGS = Setting.listSetting(
        setting("audit.logfile.events.exclude"),
        Collections.emptyList(),
        Function.identity(),
        value -> AuditLevel.parse(Collections.emptyList(), value),
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<Boolean> INCLUDE_REQUEST_BODY = Setting.boolSetting(
        setting("audit.logfile.events.emit_request_body"),
        false,
        Property.NodeScope,
        Property.Dynamic
    );
    // actions (and their requests) that are audited as "security change" events
    public static final Set<String> SECURITY_CHANGE_ACTIONS = new HashSet<>(
        Arrays.asList(
            PutUserAction.NAME,
            PutRoleAction.NAME,
            PutRoleMappingAction.NAME,
            SetEnabledAction.NAME,
            ChangePasswordAction.NAME,
            CreateApiKeyAction.NAME,
            GrantApiKeyAction.NAME,
            PutPrivilegesAction.NAME,
            DeleteUserAction.NAME,
            DeleteRoleAction.NAME,
            DeleteRoleMappingAction.NAME,
            InvalidateApiKeyAction.NAME,
            DeletePrivilegesAction.NAME,
            CreateServiceAccountTokenAction.NAME,
            DeleteServiceAccountTokenAction.NAME
        )
    );
    private static final String FILTER_POLICY_PREFIX = setting("audit.logfile.events.ignore_filters.");
    // because of the default wildcard value (*) for the field filter, a policy with
    // an unspecified filter field will match events that have any value for that
    // particular field, as well as events with that particular field missing
    protected static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_PRINCIPALS = Setting.affixKeySetting(
        FILTER_POLICY_PREFIX,
        "users",
        (key) -> Setting.listSetting(
            key,
            Collections.singletonList("*"),
            Function.identity(),
            value -> EventFilterPolicy.parsePredicate(value),
            Property.NodeScope,
            Property.Dynamic
        )
    );
    protected static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_REALMS = Setting.affixKeySetting(
        FILTER_POLICY_PREFIX,
        "realms",
        (key) -> Setting.listSetting(
            key,
            Collections.singletonList("*"),
            Function.identity(),
            value -> EventFilterPolicy.parsePredicate(value),
            Property.NodeScope,
            Property.Dynamic
        )
    );
    protected static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_ROLES = Setting.affixKeySetting(
        FILTER_POLICY_PREFIX,
        "roles",
        (key) -> Setting.listSetting(
            key,
            Collections.singletonList("*"),
            Function.identity(),
            value -> EventFilterPolicy.parsePredicate(value),
            Property.NodeScope,
            Property.Dynamic
        )
    );
    protected static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_INDICES = Setting.affixKeySetting(
        FILTER_POLICY_PREFIX,
        "indices",
        (key) -> Setting.listSetting(
            key,
            Collections.singletonList("*"),
            Function.identity(),
            value -> EventFilterPolicy.parsePredicate(value),
            Property.NodeScope,
            Property.Dynamic
        )
    );
    protected static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_ACTIONS = Setting.affixKeySetting(
        FILTER_POLICY_PREFIX,
        "actions",
        (key) -> Setting.listSetting(
            key,
            Collections.singletonList("*"),
            Function.identity(),
            value -> EventFilterPolicy.parsePredicate(value),
            Property.NodeScope,
            Property.Dynamic
        )
    );

    private static final Marker AUDIT_MARKER = MarkerManager.getMarker("org.elasticsearch.xpack.security.audit");

    private final Logger logger;
    private final ThreadContext threadContext;
    private final SecurityContext securityContext;
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
        this(settings, clusterService, LogManager.getLogger(LoggingAuditTrail.class), threadPool.getThreadContext());
    }

    LoggingAuditTrail(Settings settings, ClusterService clusterService, Logger logger, ThreadContext threadContext) {
        this.logger = logger;
        this.events = parse(INCLUDE_EVENT_SETTINGS.get(settings), EXCLUDE_EVENT_SETTINGS.get(settings));
        this.includeRequestBody = INCLUDE_REQUEST_BODY.get(settings);
        this.threadContext = threadContext;
        this.securityContext = new SecurityContext(settings, threadContext);
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
        },
            Arrays.asList(
                EMIT_HOST_ADDRESS_SETTING,
                EMIT_HOST_NAME_SETTING,
                EMIT_NODE_NAME_SETTING,
                EMIT_NODE_ID_SETTING,
                INCLUDE_EVENT_SETTINGS,
                EXCLUDE_EVENT_SETTINGS,
                INCLUDE_REQUEST_BODY
            )
        );
        clusterService.getClusterSettings()
            .addAffixGroupUpdateConsumer(
                org.elasticsearch.core.List.of(
                    FILTER_POLICY_IGNORE_PRINCIPALS,
                    FILTER_POLICY_IGNORE_REALMS,
                    FILTER_POLICY_IGNORE_ROLES,
                    FILTER_POLICY_IGNORE_INDICES,
                    FILTER_POLICY_IGNORE_ACTIONS
                ),
                (policyName, updatedSettings) -> {
                    if (updatedSettings.keySet().isEmpty()) {
                        this.eventFilterPolicyRegistry.remove(policyName);
                    } else {
                        this.eventFilterPolicyRegistry.set(policyName, new EventFilterPolicy(policyName, updatedSettings));
                    }
                }
            );

        // this log filter ensures that audit events are not filtered out because of the log level
        final LoggerContext ctx = LoggerContext.getContext(false);
        MarkerFilter auditMarkerFilter = MarkerFilter.createFilter(AUDIT_MARKER.getName(), Result.ACCEPT, Result.NEUTRAL);
        ctx.addFilter(auditMarkerFilter);
        ctx.updateLoggers();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ignored -> {
            LogManager.getLogger(Security.class).warn("Changing log level for [" + LoggingAuditTrail.class.getName() + "] has no effect");
        }, Collections.singletonList(Loggers.LOG_LEVEL_SETTING.getConcreteSettingForNamespace(LoggingAuditTrail.class.getName())));
    }

    @Override
    public void authenticationSuccess(RestRequest request) {
        final String requestId = AuditUtil.extractRequestId(securityContext.getThreadContext());
        if (requestId == null) {
            // should never happen
            throw new ElasticsearchSecurityException("Authenticated context must include request id");
        }
        final Authentication authentication;
        try {
            authentication = securityContext.getAuthentication();
        } catch (Exception e) {
            logger.error(
                () -> String.format(Locale.ROOT, "caught exception while trying to read authentication from request [%s]", request),
                e
            );
            tamperedRequest(requestId, request.getHttpRequest());
            throw new ElasticsearchSecurityException("rest request attempted to inject a user", e);
        }
        if (authentication == null) {
            // should never happen
            throw new ElasticsearchSecurityException("Context is not authenticated");
        }
        if (events.contains(AUTHENTICATION_SUCCESS)
            && eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()
                    )
                ) == false) {
            // this is redundant information maintained for bwc purposes
            final String authnRealm = authentication.getAuthenticatedBy().getName();
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "authentication_success")
                .with(REALM_FIELD_NAME, authnRealm)
                .withRestUriAndMethod(request.getHttpRequest())
                .withRequestId(requestId)
                .withAuthentication(authentication)
                .withRestOrigin(threadContext)
                .withRequestBody(request)
                .withThreadContext(securityContext.getThreadContext())
                .build();
        }
    }

    @Override
    public void authenticationSuccess(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.empty(),
                        indices,
                        Optional.of(action)
                    )
                ) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "authentication_success")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withAuthentication(authentication)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, String action, TransportRequest transportRequest) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices, Optional.of(action))) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, HttpPreRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)
            && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
                .withRestUriAndMethod(request)
                .withRestOrigin(threadContext)
                .withRequestId(requestId)
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportRequest transportRequest) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), indices, Optional.of(action))) == false) {
                final LogEntryBuilder logEntryBuilder = new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                    .with(ACTION_FIELD_NAME, action)
                    .with(PRINCIPAL_FIELD_NAME, token.principal())
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext);
                if (token instanceof ServiceAccountToken) {
                    logEntryBuilder.with(SERVICE_TOKEN_NAME_FIELD_NAME, ((ServiceAccountToken) token).getTokenName());
                }
                logEntryBuilder.build();
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, HttpPreRequest request) {
        if (events.contains(AUTHENTICATION_FAILED) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                .withRestUriAndMethod(request)
                .withRestOrigin(threadContext)
                .withRequestId(requestId)
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void authenticationFailed(String requestId, String action, TransportRequest transportRequest) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices, Optional.of(action))) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, HttpPreRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)
            && eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), Optional.empty(), Optional.empty())) == false) {
            final LogEntryBuilder logEntryBuilder = new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "authentication_failed")
                .with(PRINCIPAL_FIELD_NAME, token.principal())
                .withRestUriAndMethod(request)
                .withRestOrigin(threadContext)
                .withRequestId(requestId)
                .withThreadContext(threadContext);
            if (token instanceof ServiceAccountToken) {
                logEntryBuilder.with(SERVICE_TOKEN_NAME_FIELD_NAME, ((ServiceAccountToken) token).getTokenName());
            }
            logEntryBuilder.build();
        }
    }

    @Override
    public void authenticationFailed(
        String requestId,
        String realm,
        AuthenticationToken token,
        String action,
        TransportRequest transportRequest
    ) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), indices, Optional.of(action))) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                    .with(REALM_FIELD_NAME, realm)
                    .with(PRINCIPAL_FIELD_NAME, token.principal())
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, HttpPreRequest request) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)
            && eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), Optional.empty(), Optional.empty())) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                .with(REALM_FIELD_NAME, realm)
                .with(PRINCIPAL_FIELD_NAME, token.principal())
                .withRestUriAndMethod(request)
                .withRestOrigin(threadContext)
                .withRequestId(requestId)
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void accessGranted(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest msg,
        AuthorizationInfo authorizationInfo
    ) {
        final User user = authentication.getUser();
        final boolean isSystem = User.isInternal(user);
        if ((isSystem && events.contains(SYSTEM_ACCESS_GRANTED)) || ((isSystem == false) && events.contains(ACCESS_GRANTED))) {
            final Optional<String[]> indices = indices(msg);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(user),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.of(authorizationInfo),
                        indices,
                        Optional.of(action)
                    )
                ) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "access_granted")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, msg.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withAuthentication(authentication)
                    .withRestOrTransportOrigin(msg, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .with(authorizationInfo.asMap())
                    .build();
            }
        }
        // "Security config change" records are not filtered out by ignore policies (i.e. they are always printed).
        // The security changes here are the consequences of *user* requests,
        // so in a strict interpretation we should filter them out if there are ignore policies in place for the causing user,
        // but we do NOT do that because filtering out audit records of security changes can be unexpectedly dangerous.
        if (events.contains(SECURITY_CONFIG_CHANGE) && SECURITY_CHANGE_ACTIONS.contains(action)) {
            try {
                if (msg instanceof PutUserRequest) {
                    assert PutUserAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((PutUserRequest) msg).build();
                } else if (msg instanceof PutRoleRequest) {
                    assert PutRoleAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((PutRoleRequest) msg).build();
                } else if (msg instanceof PutRoleMappingRequest) {
                    assert PutRoleMappingAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((PutRoleMappingRequest) msg).build();
                } else if (msg instanceof SetEnabledRequest) {
                    assert SetEnabledAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((SetEnabledRequest) msg).build();
                } else if (msg instanceof ChangePasswordRequest) {
                    assert ChangePasswordAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((ChangePasswordRequest) msg).build();
                } else if (msg instanceof CreateApiKeyRequest) {
                    assert CreateApiKeyAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((CreateApiKeyRequest) msg).build();
                } else if (msg instanceof GrantApiKeyRequest) {
                    assert GrantApiKeyAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((GrantApiKeyRequest) msg).build();
                } else if (msg instanceof PutPrivilegesRequest) {
                    assert PutPrivilegesAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((PutPrivilegesRequest) msg).build();
                } else if (msg instanceof DeleteUserRequest) {
                    assert DeleteUserAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((DeleteUserRequest) msg).build();
                } else if (msg instanceof DeleteRoleRequest) {
                    assert DeleteRoleAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((DeleteRoleRequest) msg).build();
                } else if (msg instanceof DeleteRoleMappingRequest) {
                    assert DeleteRoleMappingAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((DeleteRoleMappingRequest) msg).build();
                } else if (msg instanceof InvalidateApiKeyRequest) {
                    assert InvalidateApiKeyAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((InvalidateApiKeyRequest) msg).build();
                } else if (msg instanceof DeletePrivilegesRequest) {
                    assert DeletePrivilegesAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((DeletePrivilegesRequest) msg).build();
                } else if (msg instanceof CreateServiceAccountTokenRequest) {
                    assert CreateServiceAccountTokenAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((CreateServiceAccountTokenRequest) msg).build();
                } else if (msg instanceof DeleteServiceAccountTokenRequest) {
                    assert DeleteServiceAccountTokenAction.NAME.equals(action);
                    securityChangeLogEntryBuilder(requestId).withRequestBody((DeleteServiceAccountTokenRequest) msg).build();
                } else {
                    throw new IllegalStateException(
                        "Unknown message class type ["
                            + msg.getClass().getSimpleName()
                            + "] for the \"security change\" action ["
                            + action
                            + "]"
                    );
                }
            } catch (IOException e) {
                throw new ElasticsearchSecurityException("Unexpected error while serializing event data", e);
            }
        }
    }

    @Override
    public void explicitIndexAccessEvent(
        String requestId,
        AuditLevel eventType,
        Authentication authentication,
        String action,
        String index,
        String requestName,
        TransportAddress remoteAddress,
        AuthorizationInfo authorizationInfo
    ) {
        assert eventType == ACCESS_DENIED || eventType == AuditLevel.ACCESS_GRANTED || eventType == SYSTEM_ACCESS_GRANTED;
        final String[] indices = index == null ? null : new String[] { index };
        final User user = authentication.getUser();
        if (User.isInternal(user) && eventType == ACCESS_GRANTED) {
            eventType = SYSTEM_ACCESS_GRANTED;
        }
        if (events.contains(eventType)) {
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(user),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.of(authorizationInfo),
                        Optional.ofNullable(indices),
                        Optional.of(action)
                    )
                ) == false) {
                final LogEntryBuilder logEntryBuilder = new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, eventType == ACCESS_DENIED ? "access_denied" : "access_granted")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, requestName)
                    .withRequestId(requestId)
                    .withAuthentication(authentication)
                    .with(INDICES_FIELD_NAME, indices)
                    .withThreadContext(threadContext)
                    .with(authorizationInfo.asMap());
                final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
                if (restAddress != null) {
                    logEntryBuilder.with(ORIGIN_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                        .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
                } else if (remoteAddress != null) {
                    logEntryBuilder.with(ORIGIN_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                        .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(remoteAddress.address()));
                }
                logEntryBuilder.build();
            }
        }
    }

    @Override
    public void accessDenied(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    ) {
        if (events.contains(ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.of(authorizationInfo),
                        indices,
                        Optional.of(action)
                    )
                ) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "access_denied")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withAuthentication(authentication)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .with(authorizationInfo.asMap())
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, HttpPreRequest request) {
        if (events.contains(TAMPERED_REQUEST) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "tampered_request")
                .withRestUriAndMethod(request)
                .withRestOrigin(threadContext)
                .withRequestId(requestId)
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void tamperedRequest(String requestId, String action, TransportRequest transportRequest) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices, Optional.of(action))) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "tampered_request")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, Authentication authentication, String action, TransportRequest transportRequest) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.empty(),
                        indices,
                        Optional.of(action)
                    )
                ) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "tampered_request")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .withAuthentication(authentication)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void connectionGranted(InetSocketAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, IP_FILTER_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "connection_granted")
                .with(
                    ORIGIN_TYPE_FIELD_NAME,
                    IPFilter.HTTP_PROFILE_NAME.equals(profile) ? REST_ORIGIN_FIELD_VALUE : TRANSPORT_ORIGIN_FIELD_VALUE
                )
                .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
                .with(TRANSPORT_PROFILE_FIELD_NAME, profile)
                .with(RULE_FIELD_NAME, rule.toString())
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void connectionDenied(InetSocketAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED) && eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, IP_FILTER_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "connection_denied")
                .with(
                    ORIGIN_TYPE_FIELD_NAME,
                    IPFilter.HTTP_PROFILE_NAME.equals(profile) ? REST_ORIGIN_FIELD_VALUE : TRANSPORT_ORIGIN_FIELD_VALUE
                )
                .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
                .with(TRANSPORT_PROFILE_FIELD_NAME, profile)
                .with(RULE_FIELD_NAME, rule.toString())
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void runAsGranted(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    ) {
        if (events.contains(RUN_AS_GRANTED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.of(authorizationInfo),
                        indices,
                        Optional.of(action)
                    )
                ) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "run_as_granted")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRunAsSubject(authentication)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .with(authorizationInfo.asMap())
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void runAsDenied(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        AuthorizationInfo authorizationInfo
    ) {
        if (events.contains(RUN_AS_DENIED)) {
            final Optional<String[]> indices = indices(transportRequest);
            if (eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.of(authorizationInfo),
                        indices,
                        Optional.of(action)
                    )
                ) == false) {
                new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, TRANSPORT_ORIGIN_FIELD_VALUE)
                    .with(EVENT_ACTION_FIELD_NAME, "run_as_denied")
                    .with(ACTION_FIELD_NAME, action)
                    .with(REQUEST_NAME_FIELD_NAME, transportRequest.getClass().getSimpleName())
                    .withRequestId(requestId)
                    .withRunAsSubject(authentication)
                    .withRestOrTransportOrigin(transportRequest, threadContext)
                    .with(INDICES_FIELD_NAME, indices.orElse(null))
                    .with(authorizationInfo.asMap())
                    .withThreadContext(threadContext)
                    .build();
            }
        }
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, HttpPreRequest request, AuthorizationInfo authorizationInfo) {
        if (events.contains(RUN_AS_DENIED)
            && eventFilterPolicyRegistry.ignorePredicate()
                .test(
                    new AuditEventMetaInfo(
                        Optional.of(authentication.getUser()),
                        // can be null for API keys created before version 7.7
                        Optional.ofNullable(ApiKeyService.getCreatorRealmName(authentication)),
                        Optional.of(authorizationInfo),
                        Optional.empty(),
                        Optional.empty()
                    )
                ) == false) {
            new LogEntryBuilder().with(EVENT_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                .with(EVENT_ACTION_FIELD_NAME, "run_as_denied")
                .with(authorizationInfo.asMap())
                .withRestUriAndMethod(request)
                .withRunAsSubject(authentication)
                .withRestOrigin(threadContext)
                .withRequestId(requestId)
                .withThreadContext(threadContext)
                .build();
        }
    }

    @Override
    public void coordinatingActionResponse(
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest transportRequest,
        TransportResponse transportResponse
    ) {
        // not implemented yet
    }

    private LogEntryBuilder securityChangeLogEntryBuilder(String requestId) {
        return new LogEntryBuilder(false).with(EVENT_TYPE_FIELD_NAME, SECURITY_CHANGE_ORIGIN_FIELD_VALUE).withRequestId(requestId);
    }

    private class LogEntryBuilder {

        private final StringMapMessage logEntry;

        LogEntryBuilder() {
            this(true);
        }

        LogEntryBuilder(boolean showOrigin) {
            logEntry = new StringMapMessage(LoggingAuditTrail.this.entryCommonFields.commonFields);
            if (false == showOrigin) {
                logEntry.remove(ORIGIN_ADDRESS_FIELD_NAME);
                logEntry.remove(ORIGIN_TYPE_FIELD_NAME);
            }
        }

        LogEntryBuilder withRequestBody(PutUserRequest putUserRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "put_user");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("user")
                .field("name", putUserRequest.username())
                .field("enabled", putUserRequest.enabled())
                .array("roles", putUserRequest.roles());
            if (putUserRequest.fullName() != null) {
                builder.field("full_name", putUserRequest.fullName());
            }
            if (putUserRequest.email() != null) {
                builder.field("email", putUserRequest.email());
            }
            // password and password hashes are not exposed in the audit log
            builder.field("has_password", putUserRequest.passwordHash() != null);
            if (putUserRequest.metadata() != null && false == putUserRequest.metadata().isEmpty()) {
                // JSON building for the metadata might fail when encountering unknown class types.
                // This is NOT a problem because such metadata (eg containing GeoPoint) will most probably
                // cause troubles in downstream code (eg storing the metadata), so this simply introduces a new failure mode.
                // Also the malevolent metadata can only be produced by the transport client.
                builder.field("metadata", putUserRequest.metadata());
            }
            builder.endObject() // user
                .endObject();
            logEntry.with(PUT_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(ChangePasswordRequest changePasswordRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "change_password");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("password")
                .startObject("user")
                .field("name", changePasswordRequest.username())
                .endObject() // user
                .endObject() // password
                .endObject();
            logEntry.with(CHANGE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(PutRoleRequest putRoleRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "put_role");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("role")
                .field("name", putRoleRequest.name())
                // the "role_descriptor" nested structure, where the "name" is left out, is closer to the event structure
                // for creating API Keys
                .field("role_descriptor");
            withRoleDescriptor(builder, putRoleRequest.roleDescriptor());
            builder.endObject() // role
                .endObject();
            logEntry.with(PUT_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(PutRoleMappingRequest putRoleMappingRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "put_role_mapping");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject().startObject("role_mapping").field("name", putRoleMappingRequest.getName());
            if (putRoleMappingRequest.getRoles() != null && false == putRoleMappingRequest.getRoles().isEmpty()) {
                builder.field("roles", putRoleMappingRequest.getRoles());
            }
            if (putRoleMappingRequest.getRoleTemplates() != null && false == putRoleMappingRequest.getRoleTemplates().isEmpty()) {
                // the toXContent method of the {@code TemplateRoleName} does a good job
                builder.field("role_templates", putRoleMappingRequest.getRoleTemplates());
            }
            // the toXContent methods of the {@code RoleMapperExpression} instances do a good job
            builder.field("rules", putRoleMappingRequest.getRules()).field("enabled", putRoleMappingRequest.isEnabled());
            if (putRoleMappingRequest.getMetadata() != null && false == putRoleMappingRequest.getMetadata().isEmpty()) {
                builder.field("metadata", putRoleMappingRequest.getMetadata());
            }
            builder.endObject() // role_mapping
                .endObject();
            logEntry.with(PUT_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(SetEnabledRequest setEnabledRequest) throws IOException {
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            // setEnabledRequest#enabled cannot be `null`, but nevertheless we should not assume it at this layer
            if (setEnabledRequest.enabled() != null && setEnabledRequest.enabled()) {
                builder.startObject()
                    .startObject("enable")
                    .startObject("user")
                    .field("name", setEnabledRequest.username())
                    .endObject() // user
                    .endObject() // enable
                    .endObject();
                logEntry.with(EVENT_ACTION_FIELD_NAME, "change_enable_user");
            } else {
                builder.startObject()
                    .startObject("disable")
                    .startObject("user")
                    .field("name", setEnabledRequest.username())
                    .endObject() // user
                    .endObject() // disable
                    .endObject();
                logEntry.with(EVENT_ACTION_FIELD_NAME, "change_disable_user");
            }
            logEntry.with(CHANGE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(PutPrivilegesRequest putPrivilegesRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "put_privileges");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                // toXContent of {@code ApplicationPrivilegeDescriptor} does a good job
                .field("privileges", putPrivilegesRequest.getPrivileges())
                .endObject();
            logEntry.with(PUT_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(CreateApiKeyRequest createApiKeyRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "create_apikey");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject();
            withRequestBody(builder, createApiKeyRequest);
            builder.endObject();
            logEntry.with(CREATE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(GrantApiKeyRequest grantApiKeyRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "create_apikey");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject();
            withRequestBody(builder, grantApiKeyRequest.getApiKeyRequest());
            GrantApiKeyRequest.Grant grant = grantApiKeyRequest.getGrant();
            builder.startObject("grant").field("type", grant.getType());
            if (grant.getUsername() != null) {
                builder.startObject("user")
                    .field("name", grant.getUsername())
                    .field("has_password", grant.getPassword() != null)
                    .endObject(); // user
            }
            if (grant.getAccessToken() != null) {
                builder.field("has_access_token", grant.getAccessToken() != null);
            }
            builder.endObject(); // grant
            builder.endObject();
            logEntry.with(CREATE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        private void withRequestBody(XContentBuilder builder, CreateApiKeyRequest createApiKeyRequest) throws IOException {
            TimeValue expiration = createApiKeyRequest.getExpiration();
            builder.startObject("apikey")
                .field("name", createApiKeyRequest.getName())
                .field("expiration", expiration != null ? expiration.toString() : null)
                .startArray("role_descriptors");
            for (RoleDescriptor roleDescriptor : createApiKeyRequest.getRoleDescriptors()) {
                withRoleDescriptor(builder, roleDescriptor);
            }
            builder.endArray() // role_descriptors
                .endObject(); // apikey
        }

        private void withRoleDescriptor(XContentBuilder builder, RoleDescriptor roleDescriptor) throws IOException {
            builder.startObject().array(RoleDescriptor.Fields.CLUSTER.getPreferredName(), roleDescriptor.getClusterPrivileges());
            if (roleDescriptor.getConditionalClusterPrivileges() != null && roleDescriptor.getConditionalClusterPrivileges().length > 0) {
                // This fails if this list contains multiple instances of the {@code ManageApplicationPrivileges}
                // Again, only the transport client can produce this, and this only introduces a different failure mode and
                // not a new one (i.e. without auditing it would fail differently, but it would still fail)
                builder.field(RoleDescriptor.Fields.GLOBAL.getPreferredName());
                ConfigurableClusterPrivileges.toXContent(
                    builder,
                    ToXContent.EMPTY_PARAMS,
                    Arrays.asList(roleDescriptor.getConditionalClusterPrivileges())
                );
            }
            builder.startArray(RoleDescriptor.Fields.INDICES.getPreferredName());
            for (RoleDescriptor.IndicesPrivileges indicesPrivileges : roleDescriptor.getIndicesPrivileges()) {
                withIndicesPrivileges(builder, indicesPrivileges);
            }
            builder.endArray();
            // the toXContent method of the {@code RoleDescriptor.ApplicationResourcePrivileges) does a good job
            builder.xContentList(RoleDescriptor.Fields.APPLICATIONS.getPreferredName(), roleDescriptor.getApplicationPrivileges());
            builder.array(RoleDescriptor.Fields.RUN_AS.getPreferredName(), roleDescriptor.getRunAs());
            if (roleDescriptor.getMetadata() != null && false == roleDescriptor.getMetadata().isEmpty()) {
                // JSON building for the metadata might fail when encountering unknown class types.
                // This is NOT a problem because such metadata (eg containing GeoPoint) will most probably
                // cause troubles in downstream code (eg storing the metadata), so this simply introduces a new failure mode.
                // Also the malevolent metadata can only be produced by the transport client.
                builder.field(RoleDescriptor.Fields.METADATA.getPreferredName(), roleDescriptor.getMetadata());
            }
            builder.endObject();
        }

        private void withIndicesPrivileges(XContentBuilder builder, RoleDescriptor.IndicesPrivileges indicesPrivileges) throws IOException {
            builder.startObject();
            builder.array("names", indicesPrivileges.getIndices());
            builder.array("privileges", indicesPrivileges.getPrivileges());
            if (indicesPrivileges.isUsingFieldLevelSecurity()) {
                builder.startObject(RoleDescriptor.Fields.FIELD_PERMISSIONS.getPreferredName());
                // always print the "grant" fields (even if the placeholder for all) because it looks better when avoiding the sole
                // "except" field
                builder.array(RoleDescriptor.Fields.GRANT_FIELDS.getPreferredName(), indicesPrivileges.getGrantedFields());
                if (indicesPrivileges.hasDeniedFields()) {
                    builder.array(RoleDescriptor.Fields.EXCEPT_FIELDS.getPreferredName(), indicesPrivileges.getDeniedFields());
                }
                builder.endObject();
            }
            if (indicesPrivileges.isUsingDocumentLevelSecurity()) {
                builder.field("query", indicesPrivileges.getQuery().utf8ToString());
            }
            // default for "allow_restricted_indices" is false, and it's very common to stay that way, so don't show it unless true
            if (indicesPrivileges.allowRestrictedIndices()) {
                builder.field("allow_restricted_indices", indicesPrivileges.allowRestrictedIndices());
            }
            builder.endObject();
        }

        LogEntryBuilder withRequestBody(DeleteUserRequest deleteUserRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "delete_user");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("user")
                .field("name", deleteUserRequest.username())
                .endObject() // user
                .endObject();
            logEntry.with(DELETE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(DeleteRoleRequest deleteRoleRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "delete_role");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("role")
                .field("name", deleteRoleRequest.name())
                .endObject() // role
                .endObject();
            logEntry.with(DELETE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(DeleteRoleMappingRequest deleteRoleMappingRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "delete_role_mapping");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("role_mapping")
                .field("name", deleteRoleMappingRequest.getName())
                .endObject() // role_mapping
                .endObject();
            logEntry.with(DELETE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(InvalidateApiKeyRequest invalidateApiKeyRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "invalidate_apikeys");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject().startObject("apikeys");
            if (invalidateApiKeyRequest.getIds() != null && invalidateApiKeyRequest.getIds().length > 0) {
                builder.array("ids", invalidateApiKeyRequest.getIds());
            }
            if (Strings.hasLength(invalidateApiKeyRequest.getName())) {
                builder.field("name", invalidateApiKeyRequest.getName());
            }
            builder.field("owned_by_authenticated_user", invalidateApiKeyRequest.ownedByAuthenticatedUser());
            if (Strings.hasLength(invalidateApiKeyRequest.getUserName()) || Strings.hasLength(invalidateApiKeyRequest.getRealmName())) {
                builder.startObject("user")
                    .field("name", invalidateApiKeyRequest.getUserName())
                    .field("realm", invalidateApiKeyRequest.getRealmName())
                    .endObject(); // user
            }
            builder.endObject() // apikeys
                .endObject();
            logEntry.with(INVALIDATE_API_KEYS_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(DeletePrivilegesRequest deletePrivilegesRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "delete_privileges");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("privileges")
                .field("application", deletePrivilegesRequest.application())
                .array("privileges", deletePrivilegesRequest.privileges())
                .endObject() // privileges
                .endObject();
            logEntry.with(DELETE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(CreateServiceAccountTokenRequest createServiceAccountTokenRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "create_service_token");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("service_token")
                .field("namespace", createServiceAccountTokenRequest.getNamespace())
                .field("service", createServiceAccountTokenRequest.getServiceName())
                .field("name", createServiceAccountTokenRequest.getTokenName())
                .endObject() // service_token
                .endObject();
            logEntry.with(CREATE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRequestBody(DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest) throws IOException {
            logEntry.with(EVENT_ACTION_FIELD_NAME, "delete_service_token");
            XContentBuilder builder = JsonXContent.contentBuilder().humanReadable(true);
            builder.startObject()
                .startObject("service_token")
                .field("namespace", deleteServiceAccountTokenRequest.getNamespace())
                .field("service", deleteServiceAccountTokenRequest.getServiceName())
                .field("name", deleteServiceAccountTokenRequest.getTokenName())
                .endObject() // service_token
                .endObject();
            logEntry.with(DELETE_CONFIG_FIELD_NAME, Strings.toString(builder));
            return this;
        }

        LogEntryBuilder withRestUriAndMethod(HttpPreRequest request) {
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

        LogEntryBuilder withRestOrigin(ThreadContext threadContext) {
            assert LOCAL_ORIGIN_FIELD_VALUE.equals(logEntry.get(ORIGIN_TYPE_FIELD_NAME)); // this is the default
            final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
            if (restAddress != null) {
                logEntry.with(ORIGIN_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
            }
            // fall through to local_node default
            return this;
        }

        LogEntryBuilder withRestOrTransportOrigin(TransportRequest transportRequest, ThreadContext threadContext) {
            assert LOCAL_ORIGIN_FIELD_VALUE.equals(logEntry.get(ORIGIN_TYPE_FIELD_NAME)); // this is the default
            final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
            if (restAddress != null) {
                logEntry.with(ORIGIN_TYPE_FIELD_NAME, REST_ORIGIN_FIELD_VALUE)
                    .with(ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
            } else {
                final TransportAddress address = transportRequest.remoteAddress();
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

        LogEntryBuilder withThreadContext(ThreadContext threadContext) {
            setThreadContextField(threadContext, AuditTrail.X_FORWARDED_FOR_HEADER, X_FORWARDED_FOR_FIELD_NAME);
            setThreadContextField(threadContext, Task.X_OPAQUE_ID_HTTP_HEADER, OPAQUE_ID_FIELD_NAME);
            setThreadContextField(threadContext, Task.TRACE_ID, TRACE_ID_FIELD_NAME);
            return this;
        }

        private void setThreadContextField(ThreadContext threadContext, String threadContextFieldName, String auditLogFieldName) {
            final String fieldValue = threadContext.getHeader(threadContextFieldName);
            if (fieldValue != null) {
                logEntry.with(auditLogFieldName, fieldValue);
            }
        }

        LogEntryBuilder withAuthentication(Authentication authentication) {
            logEntry.with(PRINCIPAL_FIELD_NAME, authentication.getUser().principal());
            logEntry.with(AUTHENTICATION_TYPE_FIELD_NAME, authentication.getAuthenticationType().toString());
            if (Authentication.AuthenticationType.API_KEY == authentication.getAuthenticationType()) {
                logEntry.with(API_KEY_ID_FIELD_NAME, (String) authentication.getMetadata().get(ApiKeyService.API_KEY_ID_KEY));
                String apiKeyName = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_NAME_KEY);
                if (apiKeyName != null) {
                    logEntry.with(API_KEY_NAME_FIELD_NAME, apiKeyName);
                }
                String creatorRealmName = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_NAME);
                if (creatorRealmName != null) {
                    // can be null for API keys created before version 7.7
                    logEntry.with(PRINCIPAL_REALM_FIELD_NAME, creatorRealmName);
                }
            } else {
                if (authentication.getUser().isRunAs()) {
                    logEntry.with(PRINCIPAL_REALM_FIELD_NAME, authentication.getLookedUpBy().getName())
                        .with(PRINCIPAL_RUN_BY_FIELD_NAME, authentication.getUser().authenticatedUser().principal())
                        .with(PRINCIPAL_RUN_BY_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
                } else {
                    logEntry.with(PRINCIPAL_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
                }
            }
            if (authentication.isServiceAccount()) {
                logEntry.with(SERVICE_TOKEN_NAME_FIELD_NAME, (String) authentication.getMetadata().get(TOKEN_NAME_FIELD))
                    .with(
                        SERVICE_TOKEN_TYPE_FIELD_NAME,
                        ServiceAccountSettings.REALM_TYPE + "_" + authentication.getMetadata().get(TOKEN_SOURCE_FIELD)
                    );
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

        void build() {
            logger.info(AUDIT_MARKER, logEntry);
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

    private static Optional<String[]> indices(TransportRequest transportRequest) {
        if (transportRequest instanceof IndicesRequest) {
            final String[] indices = ((IndicesRequest) transportRequest).indices();
            if (indices != null) {
                return Optional.of(((IndicesRequest) transportRequest).indices());
            }
        }
        return Optional.empty();
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
        settings.add(FILTER_POLICY_IGNORE_ACTIONS);
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
        private final Predicate<String> ignoreActionsPredicate;

        /**
         * An empty filter list for a field will match events with that field missing.
         * An event with an undefined field has the field value the empty string ("") or
         * a singleton list of the empty string ([""]).
         */
        EventFilterPolicy(String name, Settings settings) {
            this.name = name;
            // "null" values are "unexpected" and should not match any ignore policy
            this.ignorePrincipalsPredicate = parsePredicate(
                FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace(name).get(settings)
            );
            this.ignoreRealmsPredicate = parsePredicate(FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace(name).get(settings));
            this.ignoreRolesPredicate = parsePredicate(FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace(name).get(settings));
            this.ignoreIndicesPredicate = parsePredicate(FILTER_POLICY_IGNORE_INDICES.getConcreteSettingForNamespace(name).get(settings));
            this.ignoreActionsPredicate = parsePredicate(FILTER_POLICY_IGNORE_ACTIONS.getConcreteSettingForNamespace(name).get(settings));
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
            return eventInfo -> {
                return eventInfo.principal != null
                    && ignorePrincipalsPredicate.test(eventInfo.principal)
                    && eventInfo.realm != null
                    && ignoreRealmsPredicate.test(eventInfo.realm)
                    && eventInfo.action != null
                    && ignoreActionsPredicate.test(eventInfo.action)
                    && eventInfo.roles.get().allMatch(role -> role != null && ignoreRolesPredicate.test(role))
                    && eventInfo.indices.get().allMatch(index -> index != null && ignoreIndicesPredicate.test(index));
            };
        }

        @Override
        public String toString() {
            return "[users]:"
                + ignorePrincipalsPredicate.toString()
                + "&[realms]:"
                + ignoreRealmsPredicate.toString()
                + "&[roles]:"
                + ignoreRolesPredicate.toString()
                + "&[indices]:"
                + ignoreIndicesPredicate.toString()
                + "&[actions]:"
                + ignoreActionsPredicate.toString();
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
            final MapBuilder<String, EventFilterPolicy> mapBuilder = MapBuilder.newMapBuilder();
            for (final String policyName : settings.getGroups(FILTER_POLICY_PREFIX, true).keySet()) {
                mapBuilder.put(policyName, new EventFilterPolicy(policyName, settings));
            }
            policyMap = mapBuilder.immutableMap();
            // precompute predicate
            predicate = buildIgnorePredicate(policyMap);
        }

        private synchronized void set(String policyName, EventFilterPolicy eventFilterPolicy) {
            policyMap = MapBuilder.newMapBuilder(policyMap).put(policyName, eventFilterPolicy).immutableMap();
            // precompute predicate
            predicate = buildIgnorePredicate(policyMap);
        }

        private synchronized void remove(String policyName) {
            policyMap = Maps.copyMapWithRemovedEntry(policyMap, policyName);
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
        final String action;
        final Supplier<Stream<String>> roles;
        final Supplier<Stream<String>> indices;

        // empty is used for events can be filtered out only by the lack of a field
        static final AuditEventMetaInfo EMPTY = new AuditEventMetaInfo(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );

        /**
         * If a field is missing for an event, its value for filtering purposes is the
         * empty string or a singleton stream of the empty string. This a allows a
         * policy to filter by the missing value using the empty string, ie
         * `ignore_filters.users: ["", "elastic"]` will filter events with a missing
         * user field (such as `anonymous_access_denied`) as well as events from the
         * "elastic" username.
         */
        AuditEventMetaInfo(
            Optional<User> user,
            Optional<String> realm,
            Optional<AuthorizationInfo> authorizationInfo,
            Optional<String[]> indices,
            Optional<String> action
        ) {
            this.principal = user.map(u -> u.principal()).orElse("");
            this.realm = realm.orElse("");
            this.action = action.orElse("");
            // Supplier indirection and lazy generation of Streams serves 2 purposes:
            // 1. streams might not get generated due to short circuiting logical
            // conditions on the `principal` and `realm` fields
            // 2. reusability of the AuditEventMetaInfo instance: in this case Streams have
            // to be regenerated as they cannot be operated upon twice
            this.roles = () -> authorizationInfo.filter(info -> {
                final Object value = info.asMap().get("user.roles");
                return value instanceof String[]
                    && ((String[]) value).length != 0
                    && Arrays.stream((String[]) value).anyMatch(Objects::nonNull);
            }).map(info -> Arrays.stream((String[]) info.asMap().get("user.roles"))).orElse(Stream.of(""));
            this.indices = () -> indices.filter(i -> i.length > 0)
                .filter(a -> Arrays.stream(a).anyMatch(Objects::nonNull))
                .map(Arrays::stream)
                .orElse(Stream.of(""));
        }

        AuditEventMetaInfo(
            Optional<AuthenticationToken> authenticationToken,
            Optional<String> realm,
            Optional<String[]> indices,
            Optional<String> action
        ) {
            this.principal = authenticationToken.map(u -> u.principal()).orElse("");
            this.realm = realm.orElse("");
            this.action = action.orElse("");
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
