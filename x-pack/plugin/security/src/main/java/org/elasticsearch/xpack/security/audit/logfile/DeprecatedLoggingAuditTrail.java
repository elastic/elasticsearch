/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.AuditEventMetaInfo;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.EventFilterPolicy;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.EventFilterPolicyRegistry;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
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

public class DeprecatedLoggingAuditTrail extends AbstractComponent implements AuditTrail, ClusterStateListener {

    public static final String NAME = "deprecatedLogfile";

    private final Logger logger;
    final EventFilterPolicyRegistry eventFilterPolicyRegistry;
    private final ThreadContext threadContext;
    // package for testing
    volatile EnumSet<AuditLevel> events;
    boolean includeRequestBody;
    LocalNodeInfo localNodeInfo;

    @Override
    public String name() {
        return NAME;
    }

    public DeprecatedLoggingAuditTrail(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        this(settings, clusterService, LogManager.getLogger(DeprecatedLoggingAuditTrail.class), threadPool.getThreadContext());
    }

    DeprecatedLoggingAuditTrail(Settings settings, ClusterService clusterService, Logger logger, ThreadContext threadContext) {
        this.logger = logger;
        this.events = parse(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.get(settings), LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS.get(settings));
        this.includeRequestBody = LoggingAuditTrail.INCLUDE_REQUEST_BODY.get(settings);
        this.threadContext = threadContext;
        this.localNodeInfo = new LocalNodeInfo(settings, null);
        this.eventFilterPolicyRegistry = new EventFilterPolicyRegistry(settings);
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(newSettings -> {
            final LocalNodeInfo localNodeInfo = this.localNodeInfo;
            final Settings.Builder builder = Settings.builder().put(localNodeInfo.settings).put(newSettings, false);
            this.localNodeInfo = new LocalNodeInfo(builder.build(), localNodeInfo.localNode);
            this.includeRequestBody = LoggingAuditTrail.INCLUDE_REQUEST_BODY.get(newSettings);
            // `events` is a volatile field! Keep `events` write last so that
            // `localNodeInfo` and `includeRequestBody` writes happen-before! `events` is
            // always read before `localNodeInfo` and `includeRequestBody`.
            this.events = parse(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.get(newSettings),
                    LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS.get(newSettings));
        }, Arrays.asList(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING, LoggingAuditTrail.DEPRECATED_EMIT_HOST_ADDRESS_SETTING,
                LoggingAuditTrail.EMIT_HOST_NAME_SETTING, LoggingAuditTrail.DEPRECATED_EMIT_NODE_NAME_SETTING,
                LoggingAuditTrail.EMIT_NODE_NAME_SETTING, LoggingAuditTrail.DEPRECATED_EMIT_NODE_NAME_SETTING,
                LoggingAuditTrail.INCLUDE_EVENT_SETTINGS, LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS,
                LoggingAuditTrail.INCLUDE_REQUEST_BODY));
        clusterService.getClusterSettings().addAffixUpdateConsumer(LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS,
                (policyName, filtersList) -> {
                    final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
                    final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings))
                            .changePrincipalsFilter(filtersList);
                    this.eventFilterPolicyRegistry.set(policyName, newPolicy);
                }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
        clusterService.getClusterSettings().addAffixUpdateConsumer(LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS,
                (policyName, filtersList) -> {
                    final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
                    final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings))
                            .changeRealmsFilter(filtersList);
                    this.eventFilterPolicyRegistry.set(policyName, newPolicy);
                }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
        clusterService.getClusterSettings().addAffixUpdateConsumer(LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES,
                (policyName, filtersList) -> {
                    final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
                    final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings))
                            .changeRolesFilter(filtersList);
                    this.eventFilterPolicyRegistry.set(policyName, newPolicy);
                }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
        clusterService.getClusterSettings().addAffixUpdateConsumer(LoggingAuditTrail.FILTER_POLICY_IGNORE_INDICES,
                (policyName, filtersList) -> {
                    final Optional<EventFilterPolicy> policy = eventFilterPolicyRegistry.get(policyName);
                    final EventFilterPolicy newPolicy = policy.orElse(new EventFilterPolicy(policyName, settings))
                            .changeIndicesFilter(filtersList);
                    this.eventFilterPolicyRegistry.set(policyName, newPolicy);
                }, (policyName, filtersList) -> EventFilterPolicy.parsePredicate(filtersList));
    }

    @Override
    public void authenticationSuccess(String requestId, String realm, User user, RestRequest request) {
        if (events.contains(AUTHENTICATION_SUCCESS) && (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(realm), Optional.empty(), Optional.empty())) == false)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_success]\t{}, {}, realm=[{}], uri=[{}], params=[{}]{}, request_body=[{}]",
                        localNodeInfo.prefix, hostAttributes(request), principal(user), realm, request.uri(), request.params(), opaqueId(),
                        restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_success]\t{}, {}, realm=[{}], uri=[{}], params=[{}]{}", localNodeInfo.prefix,
                        hostAttributes(request), principal(user), realm, request.uri(), request.params(), opaqueId());
            }
        }
    }

    @Override
    public void authenticationSuccess(String requestId, String realm, User user, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(realm), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [authentication_success]\t{}, {}, realm=[{}], action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), principal(user), realm, action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [authentication_success]\t{}, {}, realm=[{}], action=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), principal(user), realm, action,
                            message.getClass().getSimpleName(), opaqueId());
                }
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, String action, TransportMessage message) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [anonymous_access_denied]\t{}, action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, message.getClass().getSimpleName(),
                            opaqueId());
                }
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String requestId, RestRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)
                && (eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [anonymous_access_denied]\t{}, uri=[{}]{}, request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), request.uri(), opaqueId(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [anonymous_access_denied]\t{}, uri=[{}]{}", localNodeInfo.prefix, hostAttributes(request),
                        request.uri(), opaqueId());
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            message.getClass().getSimpleName(), opaqueId());
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)
                && (eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_failed]\t{}, uri=[{}]{}, request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), request.uri(), opaqueId(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_failed]\t{}, uri=[{}]{}", localNodeInfo.prefix, hostAttributes(request),
                        request.uri(), opaqueId());
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [authentication_failed]\t{}, action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, message.getClass().getSimpleName(),
                            opaqueId());
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, AuthenticationToken token, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED) && (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), Optional.empty())) == false)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}]{}, request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), token.principal(), request.uri(), opaqueId(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}]{}", localNodeInfo.prefix,
                        hostAttributes(request), token.principal(), request.uri(), opaqueId());
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info(
                            "{}[transport] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], indices=[{}], "
                                    + "request=[{}]{}",
                            localNodeInfo.prefix, realm, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], request=[{}]{}",
                            localNodeInfo.prefix, realm, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            message.getClass().getSimpleName(), opaqueId());
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String requestId, String realm, AuthenticationToken token, RestRequest request) {
        if (events.contains(REALM_AUTHENTICATION_FAILED) && (eventFilterPolicyRegistry.ignorePredicate()
                .test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), Optional.empty())) == false)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}]{}, " + "request_body=[{}]",
                        localNodeInfo.prefix, realm, hostAttributes(request), token.principal(), request.uri(), opaqueId(),
                        restRequestContent(request));
            } else {
                logger.info("{}[rest] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}]{}", localNodeInfo.prefix,
                        realm, hostAttributes(request), token.principal(), request.uri(), opaqueId());
            }
        }
    }

    @Override
    public void accessGranted(String reqId, Authentication authentication, String action, TransportMessage message,
                              AuthorizationInfo authorizationInfo) {
        final User user = authentication.getUser();
        final boolean isSystem = SystemUser.is(user) || XPackUser.is(user);
        if ((isSystem && events.contains(SYSTEM_ACCESS_GRANTED)) || ((isSystem == false) && events.contains(ACCESS_GRANTED))) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(user),
                    Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                final String[] roleNames = (String[]) authorizationInfo.asMap().get(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME);
                if (indices.isPresent()) {
                    logger.info("{}[transport] [access_granted]\t{}, {}, roles=[{}], action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), subject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [access_granted]\t{}, {}, roles=[{}], action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), subject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, message.getClass().getSimpleName(), opaqueId());
                }
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
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                final String[] roleNames = (String[]) authorizationInfo.asMap().get(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME);
                if (indices.isPresent()) {
                    logger.info("{}[transport] [access_denied]\t{}, {}, roles=[{}], action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), subject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [access_denied]\t{}, {}, roles=[{}], action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), subject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, message.getClass().getSimpleName(), opaqueId());
                }
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
                final String[] roleNames = (String[]) authorizationInfo.asMap().get(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME);
                final StringBuilder logEntryBuilder = new StringBuilder();
                logEntryBuilder.append(localNodeInfo.prefix);
                logEntryBuilder.append("[transport] ");
                if (eventType == ACCESS_DENIED) {
                    logEntryBuilder.append("[access_denied]\t");
                } else {
                    logEntryBuilder.append("[access_granted]\t");
                }
                final String originAttributes = restOriginTag(threadContext).orElseGet(() -> {
                    if (remoteAddress == null) {
                        return localNodeInfo.localOriginTag;
                    }
                    return new StringBuilder("origin_type=[transport], origin_address=[")
                            .append(NetworkAddress.format(remoteAddress.address().getAddress())).append("]").toString();
                });
                logEntryBuilder.append(originAttributes).append(", ");
                logEntryBuilder.append(subject(authentication)).append(", ");
                logEntryBuilder.append("roles=[").append(arrayToCommaDelimitedString(roleNames)).append("], ");
                logEntryBuilder.append("action=[").append(action).append("], ");
                logEntryBuilder.append("indices=[").append(index).append("], ");
                logEntryBuilder.append("request=[").append(requestName).append("]");
                final String opaqueId = threadContext.getHeader(Task.X_OPAQUE_ID);
                if (opaqueId != null) {
                    logEntryBuilder.append(", opaque_id=[").append(opaqueId).append("]");
                }
                logger.info(logEntryBuilder.toString());
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, RestRequest request) {
        if (events.contains(TAMPERED_REQUEST) && (eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [tampered_request]\t{}, uri=[{}]{}, request_body=[{}]", localNodeInfo.prefix, hostAttributes(request),
                        request.uri(), opaqueId(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [tampered_request]\t{}, uri=[{}]{}", localNodeInfo.prefix, hostAttributes(request), request.uri(),
                        opaqueId());
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, String action, TransportMessage message) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [tampered_request]\t{}, action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, message.getClass().getSimpleName(),
                            opaqueId());
                }
            }
        }
    }

    @Override
    public void tamperedRequest(String requestId, User user, String action, TransportMessage request) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(request);
            if (eventFilterPolicyRegistry.ignorePredicate()
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, request, localNodeInfo), principal(user), action,
                            arrayToCommaDelimitedString(indices.get()), request.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [tampered_request]\t{}, {}, action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, request, localNodeInfo), principal(user), action,
                            request.getClass().getSimpleName(), opaqueId());
                }
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED) && (eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false)) {
            logger.info("{}[ip_filter] [connection_granted]\torigin_address=[{}], transport_profile=[{}], rule=[{}]{}",
                    localNodeInfo.prefix, NetworkAddress.format(inetAddress), profile, rule, opaqueId());
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED) && (eventFilterPolicyRegistry.ignorePredicate().test(AuditEventMetaInfo.EMPTY) == false)) {
            logger.info("{}[ip_filter] [connection_denied]\torigin_address=[{}], transport_profile=[{}], rule=[{}]{}", localNodeInfo.prefix,
                    NetworkAddress.format(inetAddress), profile, rule, opaqueId());
        }
    }

    @Override
    public void runAsGranted(String requestId, Authentication authentication, String action, TransportMessage message,
                             AuthorizationInfo authorizationInfo) {
        if (events.contains(RUN_AS_GRANTED)) {
            final Optional<String[]> indices = indices(message);
            if (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(authentication.getUser()),
                    Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                final String[] roleNames = (String[]) authorizationInfo.asMap().get(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME);
                if (indices.isPresent()) {
                    logger.info("{}[transport] [run_as_granted]\t{}, {}, roles=[{}], action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), runAsSubject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [run_as_granted]\t{}, {}, roles=[{}], action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), runAsSubject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, message.getClass().getSimpleName(), opaqueId());
                }
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
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                final String[] roleNames = (String[]) authorizationInfo.asMap().get(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME);
                if (indices.isPresent()) {
                    logger.info("{}[transport] [run_as_denied]\t{}, {}, roles=[{}], action=[{}], indices=[{}], request=[{}]{}",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), runAsSubject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName(), opaqueId());
                } else {
                    logger.info("{}[transport] [run_as_denied]\t{}, {}, roles=[{}], action=[{}], request=[{}]{}", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), runAsSubject(authentication),
                            arrayToCommaDelimitedString(roleNames), action, message.getClass().getSimpleName(), opaqueId());
                }
            }
        }
    }

    @Override
    public void runAsDenied(String requestId, Authentication authentication, RestRequest request, AuthorizationInfo authorizationInfo) {
        if (events.contains(RUN_AS_DENIED)
                && (eventFilterPolicyRegistry.ignorePredicate().test(new AuditEventMetaInfo(Optional.of(authentication.getUser()),
                        Optional.of(effectiveRealmName(authentication)), Optional.of(authorizationInfo), Optional.empty())) == false)) {
            final String[] roleNames = (String[]) authorizationInfo.asMap().get(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME);
            if (includeRequestBody) {
                logger.info("{}[rest] [run_as_denied]\t{}, {}, roles=[{}], uri=[{}], request_body=[{}]{}", localNodeInfo.prefix,
                        hostAttributes(request), runAsSubject(authentication), arrayToCommaDelimitedString(roleNames), request.uri(),
                        restRequestContent(request), opaqueId());
            } else {
                logger.info("{}[rest] [run_as_denied]\t{}, {}, roles=[{}], uri=[{}]{}", localNodeInfo.prefix, hostAttributes(request),
                        runAsSubject(authentication), arrayToCommaDelimitedString(roleNames), request.uri(), opaqueId());
            }
        }
    }

    static String runAsSubject(Authentication authentication) {
        final StringBuilder sb = new StringBuilder("principal=[");
        sb.append(authentication.getUser().authenticatedUser().principal());
        sb.append("], realm=[");
        sb.append(authentication.getAuthenticatedBy().getName());
        sb.append("], run_as_principal=[");
        sb.append(authentication.getUser().principal());
        if (authentication.getLookedUpBy() != null) {
            sb.append("], run_as_realm=[").append(authentication.getLookedUpBy().getName());
        }
        sb.append("]");
        return sb.toString();
    }

    static String subject(Authentication authentication) {
        final StringBuilder sb = new StringBuilder("principal=[");
        sb.append(authentication.getUser().principal()).append("], realm=[");
        if (authentication.getUser().isRunAs()) {
            sb.append(authentication.getLookedUpBy().getName()).append("], run_by_principal=[");
            sb.append(authentication.getUser().authenticatedUser().principal()).append("], run_by_realm=[");
        }
        sb.append(authentication.getAuthenticatedBy().getName()).append("]");
        return sb.toString();
    }

    private static String hostAttributes(RestRequest request) {
        String formattedAddress;
        final SocketAddress socketAddress = request.getRemoteAddress();
        if (socketAddress instanceof InetSocketAddress) {
            formattedAddress = NetworkAddress.format(((InetSocketAddress) socketAddress).getAddress());
        } else {
            formattedAddress = socketAddress.toString();
        }
        return "origin_address=[" + formattedAddress + "]";
    }

    protected static String originAttributes(ThreadContext threadContext, TransportMessage message, LocalNodeInfo localNodeInfo) {
        return restOriginTag(threadContext).orElse(transportOriginTag(message).orElse(localNodeInfo.localOriginTag));
    }

    private String opaqueId() {
        String opaqueId = threadContext.getHeader(Task.X_OPAQUE_ID);
        if (opaqueId != null) {
            return ", opaque_id=[" + opaqueId + "]";
        } else {
            return "";
        }
    }

    private static Optional<String> restOriginTag(ThreadContext threadContext) {
        final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress == null) {
            return Optional.empty();
        }
        return Optional.of(new StringBuilder("origin_type=[rest], origin_address=[").append(NetworkAddress.format(restAddress.getAddress()))
                .append("]").toString());
    }

    private static Optional<String> transportOriginTag(TransportMessage message) {
        final TransportAddress address = message.remoteAddress();
        if (address == null) {
            return Optional.empty();
        }
        return Optional.of(new StringBuilder("origin_type=[transport], origin_address=[")
                .append(NetworkAddress.format(address.address().getAddress())).append("]").toString());
    }

    static Optional<String[]> indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            final String[] indices = ((IndicesRequest) message).indices();
            if ((indices != null) && (indices.length != 0)) {
                return Optional.of(((IndicesRequest) message).indices());
            }
        }
        return Optional.empty();
    }

    static String effectiveRealmName(Authentication authentication) {
        return authentication.getLookedUpBy() != null ? authentication.getLookedUpBy().getName()
                : authentication.getAuthenticatedBy().getName();
    }

    static String principal(User user) {
        final StringBuilder builder = new StringBuilder("principal=[");
        builder.append(user.principal());
        if (user.isRunAs()) {
            builder.append("], run_by_principal=[").append(user.authenticatedUser().principal());
        }
        return builder.append("]").toString();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        updateLocalNodeInfo(event.state().getNodes().getLocalNode());
    }

    void updateLocalNodeInfo(DiscoveryNode newLocalNode) {
        // check if local node changed
        final LocalNodeInfo localNodeInfo = this.localNodeInfo;
        if ((localNodeInfo.localNode == null) || (localNodeInfo.localNode.equals(newLocalNode) == false)) {
            // no need to synchronize, called only from the cluster state applier thread
            this.localNodeInfo = new LocalNodeInfo(localNodeInfo.settings, newLocalNode);
        }
    }

    static class LocalNodeInfo {
        private final Settings settings;
        private final DiscoveryNode localNode;
        final String prefix;
        private final String localOriginTag;

        LocalNodeInfo(Settings settings, @Nullable DiscoveryNode newLocalNode) {
            this.settings = settings;
            this.localNode = newLocalNode;
            this.prefix = resolvePrefix(settings, newLocalNode);
            this.localOriginTag = localOriginTag(newLocalNode);
        }

        static String resolvePrefix(Settings settings, @Nullable DiscoveryNode localNode) {
            final StringBuilder builder = new StringBuilder();
            if (LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.get(settings)) {
                final String address = localNode != null ? localNode.getHostAddress() : null;
                if (address != null) {
                    builder.append("[").append(address).append("] ");
                }
            }
            if (LoggingAuditTrail.EMIT_HOST_NAME_SETTING.get(settings)) {
                final String hostName = localNode != null ? localNode.getHostName() : null;
                if (hostName != null) {
                    builder.append("[").append(hostName).append("] ");
                }
            }
            if (LoggingAuditTrail.EMIT_NODE_NAME_SETTING.get(settings)) {
                final String name = Node.NODE_NAME_SETTING.get(settings);
                if (name != null) {
                    builder.append("[").append(name).append("] ");
                }
            }
            return builder.toString();
        }

        private static String localOriginTag(@Nullable DiscoveryNode localNode) {
            if (localNode == null) {
                return "origin_type=[local_node]";
            }
            return new StringBuilder("origin_type=[local_node], origin_address=[").append(localNode.getHostAddress()).append("]")
                    .toString();
        }
    }
}
