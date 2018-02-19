/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
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

public class LoggingAuditTrail extends AbstractComponent implements AuditTrail, ClusterStateListener {

    public static final String NAME = "logfile";
    public static final Setting<Boolean> HOST_ADDRESS_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_host_address"), false, Property.NodeScope);
    public static final Setting<Boolean> HOST_NAME_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_host_name"), false, Property.NodeScope);
    public static final Setting<Boolean> NODE_NAME_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_name"), true, Property.NodeScope);
    private static final List<String> DEFAULT_EVENT_INCLUDES = Arrays.asList(
            ACCESS_DENIED.toString(),
            ACCESS_GRANTED.toString(),
            ANONYMOUS_ACCESS_DENIED.toString(),
            AUTHENTICATION_FAILED.toString(),
            CONNECTION_DENIED.toString(),
            TAMPERED_REQUEST.toString(),
            RUN_AS_DENIED.toString(),
            RUN_AS_GRANTED.toString()
    );
    private static final Setting<List<String>> INCLUDE_EVENT_SETTINGS =
            Setting.listSetting(setting("audit.logfile.events.include"), DEFAULT_EVENT_INCLUDES, Function.identity(), Property.NodeScope);
    private static final Setting<List<String>> EXCLUDE_EVENT_SETTINGS =
            Setting.listSetting(setting("audit.logfile.events.exclude"), Collections.emptyList(), Function.identity(), Property.NodeScope);
    private static final Setting<Boolean> INCLUDE_REQUEST_BODY =
            Setting.boolSetting(setting("audit.logfile.events.emit_request_body"), false, Property.NodeScope);
    private static final String FILTER_POLICY_PREFIX = setting("audit.logfile.events.ignore_filters.");
    // because of the default wildcard value (*) for the field filter, a policy with
    // an unspecified filter field will match events that have any value for that
    // particular field, as well as events with that particular field missing
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_PRINCIPALS =
            Setting.affixKeySetting(FILTER_POLICY_PREFIX, "users", (key) -> Setting.listSetting(key, Collections.singletonList("*"),
                    Function.identity(), Setting.Property.NodeScope, Setting.Property.Dynamic));
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_REALMS =
            Setting.affixKeySetting(FILTER_POLICY_PREFIX, "realms", (key) -> Setting.listSetting(key, Collections.singletonList("*"),
                    Function.identity(), Setting.Property.NodeScope, Setting.Property.Dynamic));
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_ROLES =
            Setting.affixKeySetting(FILTER_POLICY_PREFIX, "roles", (key) -> Setting.listSetting(key, Collections.singletonList("*"),
                    Function.identity(), Setting.Property.NodeScope, Setting.Property.Dynamic));
    private static final Setting.AffixSetting<List<String>> FILTER_POLICY_IGNORE_INDICES =
            Setting.affixKeySetting(FILTER_POLICY_PREFIX, "indices", (key) -> Setting.listSetting(key, Collections.singletonList("*"),
                    Function.identity(), Setting.Property.NodeScope, Setting.Property.Dynamic));

    private final Logger logger;
    private final EnumSet<AuditLevel> events;
    private final boolean includeRequestBody;
    // protected for testing
    protected final Predicate<AuditEventMetaInfo> filterPolicyPredicate;
    private final ThreadContext threadContext;
    volatile LocalNodeInfo localNodeInfo;

    @Override
    public String name() {
        return NAME;
    }

    public LoggingAuditTrail(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        this(settings, clusterService, Loggers.getLogger(LoggingAuditTrail.class), threadPool.getThreadContext());
    }

    LoggingAuditTrail(Settings settings, ClusterService clusterService, Logger logger, ThreadContext threadContext) {
        super(settings);
        this.logger = logger;
        this.events = parse(INCLUDE_EVENT_SETTINGS.get(settings), EXCLUDE_EVENT_SETTINGS.get(settings));
        this.includeRequestBody = INCLUDE_REQUEST_BODY.get(settings);
        this.threadContext = threadContext;
        this.localNodeInfo = new LocalNodeInfo(settings, null);
        this.filterPolicyPredicate = new EventFilterPolicyRegistry(settings).ignorePredicate();
        clusterService.addListener(this);
    }

    @Override
    public void authenticationSuccess(String realm, User user, RestRequest request) {
        if (events.contains(AUTHENTICATION_SUCCESS) && filterPolicyPredicate
                .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(realm), Optional.empty(), Optional.empty())) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_success]\t{}, realm=[{}], uri=[{}], params=[{}], request_body=[{}]",
                        localNodeInfo.prefix, principal(user), realm, request.uri(), request.params(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_success]\t{}, realm=[{}], uri=[{}], params=[{}]", localNodeInfo.prefix,
                        principal(user), realm, request.uri(), request.params());
            }
        }
    }

    @Override
    public void authenticationSuccess(String realm, User user, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.of(realm), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [authentication_success]\t{}, {}, realm=[{}], action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), principal(user), realm, action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [authentication_success]\t{}, {}, realm=[{}], action=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), principal(user), realm, action,
                            message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [anonymous_access_denied]\t{}, action=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED) && filterPolicyPredicate.test(AuditEventMetaInfo.EMPTY) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [anonymous_access_denied]\t{}, uri=[{}], request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [anonymous_access_denied]\t{}, uri=[{}]", localNodeInfo.prefix, hostAttributes(request),
                        request.uri());
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED) && filterPolicyPredicate.test(AuditEventMetaInfo.EMPTY) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_failed]\t{}, uri=[{}], request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_failed]\t{}, uri=[{}]", localNodeInfo.prefix, hostAttributes(request), request.uri());
            }
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [authentication_failed]\t{}, action=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)
                && filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.of(token), Optional.empty(), Optional.empty())) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}], request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), token.principal(), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}]", localNodeInfo.prefix, hostAttributes(request),
                        token.principal(), request.uri());
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info(
                            "{}[transport] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], indices=[{}], "
                                    + "request=[{}]",
                            localNodeInfo.prefix, realm, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], request=[{}]",
                            localNodeInfo.prefix, realm, originAttributes(threadContext, message, localNodeInfo), token.principal(), action,
                            message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)
                && filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.of(token), Optional.of(realm), Optional.empty())) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}], request_body=[{}]",
                        localNodeInfo.prefix, realm, hostAttributes(request), token.principal(), request.uri(),
                        restRequestContent(request));
            } else {
                logger.info("{}[rest] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}]", localNodeInfo.prefix, realm,
                        hostAttributes(request), token.principal(), request.uri());
            }
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage message, String[] roleNames) {
        final boolean isSystem = SystemUser.is(user) || XPackUser.is(user);
        if ((isSystem && events.contains(SYSTEM_ACCESS_GRANTED)) || (isSystem == false && events.contains(ACCESS_GRANTED))) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.of(roleNames), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [access_granted]\t{}, {}, roles=[{}], action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), principal(user),
                            arrayToCommaDelimitedString(roleNames), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [access_granted]\t{}, {}, roles=[{}], action=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), principal(user),
                            arrayToCommaDelimitedString(roleNames), action, message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message, String[] roleNames) {
        if (events.contains(ACCESS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.of(roleNames), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [access_denied]\t{}, {}, roles=[{}], action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo), principal(user),
                            arrayToCommaDelimitedString(roleNames), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [access_denied]\t{}, {}, roles=[{}], action=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), principal(user),
                            arrayToCommaDelimitedString(roleNames), action, message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        if (events.contains(TAMPERED_REQUEST) && filterPolicyPredicate.test(AuditEventMetaInfo.EMPTY) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [tampered_request]\t{}, uri=[{}], request_body=[{}]", localNodeInfo.prefix, hostAttributes(request),
                        request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [tampered_request]\t{}, uri=[{}]", localNodeInfo.prefix, hostAttributes(request), request.uri());
            }
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate.test(new AuditEventMetaInfo(Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, arrayToCommaDelimitedString(indices.get()),
                            message.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [tampered_request]\t{}, action=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, message, localNodeInfo), action, message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        if (events.contains(TAMPERED_REQUEST)) {
            final Optional<String[]> indices = indices(request);
            if (filterPolicyPredicate
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.empty(), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, request, localNodeInfo), principal(user), action,
                            arrayToCommaDelimitedString(indices.get()), request.getClass().getSimpleName());
                } else {
                    logger.info("{}[transport] [tampered_request]\t{}, {}, action=[{}], request=[{}]", localNodeInfo.prefix,
                            originAttributes(threadContext, request, localNodeInfo), principal(user), action,
                            request.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED) && filterPolicyPredicate.test(AuditEventMetaInfo.EMPTY) == false) {
            logger.info("{}[ip_filter] [connection_granted]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", localNodeInfo.prefix,
                    NetworkAddress.format(inetAddress), profile, rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED) && filterPolicyPredicate.test(AuditEventMetaInfo.EMPTY) == false) {
            logger.info("{}[ip_filter] [connection_denied]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", localNodeInfo.prefix,
                    NetworkAddress.format(inetAddress), profile, rule);
        }
    }

    @Override
    public void runAsGranted(User user, String action, TransportMessage message, String[] roleNames) {
        if (events.contains(RUN_AS_GRANTED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.of(roleNames), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info(
                            "{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], roles=[{}],"
                                    + " action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo),
                            user.authenticatedUser().principal(), user.principal(), arrayToCommaDelimitedString(roleNames), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName());
                } else {
                    logger.info(
                            "{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], roles=[{}],"
                                    + " action=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo),
                            user.authenticatedUser().principal(), user.principal(), arrayToCommaDelimitedString(roleNames), action,
                            message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message, String[] roleNames) {
        if (events.contains(RUN_AS_DENIED)) {
            final Optional<String[]> indices = indices(message);
            if (filterPolicyPredicate
                    .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.of(roleNames), indices)) == false) {
                final LocalNodeInfo localNodeInfo = this.localNodeInfo;
                if (indices.isPresent()) {
                    logger.info(
                            "{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], roles=[{}],"
                                    + " action=[{}], indices=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo),
                            user.authenticatedUser().principal(), user.principal(), arrayToCommaDelimitedString(roleNames), action,
                            arrayToCommaDelimitedString(indices.get()), message.getClass().getSimpleName());
                } else {
                    logger.info(
                            "{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], roles=[{}],"
                                    + " action=[{}], request=[{}]",
                            localNodeInfo.prefix, originAttributes(threadContext, message, localNodeInfo),
                            user.authenticatedUser().principal(), user.principal(), arrayToCommaDelimitedString(roleNames), action,
                            message.getClass().getSimpleName());
                }
            }
        }
    }

    @Override
    public void runAsDenied(User user, RestRequest request, String[] roleNames) {
        if (events.contains(RUN_AS_DENIED) && filterPolicyPredicate
                .test(new AuditEventMetaInfo(Optional.of(user), Optional.empty(), Optional.of(roleNames), Optional.empty())) == false) {
            if (includeRequestBody) {
                logger.info("{}[rest] [run_as_denied]\t{}, principal=[{}], roles=[{}], uri=[{}], request_body=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), user.principal(), arrayToCommaDelimitedString(roleNames), request.uri(),
                        restRequestContent(request));
            } else {
                logger.info("{}[rest] [run_as_denied]\t{}, principal=[{}], roles=[{}], uri=[{}]", localNodeInfo.prefix,
                        hostAttributes(request), user.principal(), arrayToCommaDelimitedString(roleNames), request.uri());
            }
        }
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

    private static Optional<String> restOriginTag(ThreadContext threadContext) {
        final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress == null) {
            return Optional.empty();
        }
        return Optional.of(new StringBuilder("origin_type=[rest], origin_address=[").append(NetworkAddress.format(restAddress.getAddress()))
                .append("]")
                .toString());
    }

    private static Optional<String> transportOriginTag(TransportMessage message) {
        final TransportAddress address = message.remoteAddress();
        if (address == null) {
            return Optional.empty();
        }
        return Optional.of(
                new StringBuilder("origin_type=[transport], origin_address=[").append(NetworkAddress.format(address.address().getAddress()))
                        .append("]")
                        .toString());
    }

    static Optional<String[]> indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            return Optional.ofNullable(((IndicesRequest) message).indices());
        }
        return Optional.empty();
    }

    static String principal(User user) {
        final StringBuilder builder = new StringBuilder("principal=[");
        builder.append(user.principal());
        if (user.isRunAs()) {
            builder.append("], run_by_principal=[").append(user.authenticatedUser().principal());
        }
        return builder.append("]").toString();
    }

    public static void registerSettings(List<Setting<?>> settings) {
        settings.add(HOST_ADDRESS_SETTING);
        settings.add(HOST_NAME_SETTING);
        settings.add(NODE_NAME_SETTING);
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
        final String name;
        private final Predicate<String> ignorePrincipalsPredicate;
        private final Predicate<String> ignoreRealmsPredicate;
        private final Predicate<String> ignoreRolesPredicate;
        private final Predicate<String> ignoreIndicesPredicate;

        /**
         * An empty filter list for a field will match events with that field missing.
         * An event with an undefined field has the field value the empty string ("") or
         * a singleton list of the empty string ([""]).
         */
        EventFilterPolicy(String name, Settings settings) {
            this.name = name;
            ignorePrincipalsPredicate = Automatons.predicate(
                    emptyStringBuildsEmptyAutomaton(FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace(name).get(settings)));
            ignoreRealmsPredicate = Automatons.predicate(
                    emptyStringBuildsEmptyAutomaton(FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace(name).get(settings)));
            ignoreRolesPredicate = Automatons.predicate(
                    emptyStringBuildsEmptyAutomaton(FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace(name).get(settings)));
            ignoreIndicesPredicate = Automatons.predicate(
                    emptyStringBuildsEmptyAutomaton(FILTER_POLICY_IGNORE_INDICES.getConcreteSettingForNamespace(name).get(settings)));
        }

        /**
         * It is a requirement that empty string filters match empty string fields. In
         * this case we require automatons from empty string to match the empty string.
         * `Automatons.predicate("").test("") == false`
         * `Automatons.predicate("//").test("") == true`
         */
        private List<String> emptyStringBuildsEmptyAutomaton(List<String> l) {
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
            return (eventInfo) -> {
                return ignorePrincipalsPredicate.test(eventInfo.principal)
                    && ignoreRealmsPredicate.test(eventInfo.realm)
                    && eventInfo.roles.get().allMatch(ignoreRolesPredicate)
                    && eventInfo.indices.get().allMatch(ignoreIndicesPredicate);
            };
        }
    }

    /**
     * Builds the filter predicates for all the policies. Predicates of all policies
     * are ORed together, so that an audit event matching any policy is ignored.
     */
    private static final class EventFilterPolicyRegistry {
        private final Map<String, Predicate<AuditEventMetaInfo>> policyMap;

        EventFilterPolicyRegistry(Settings settings) {
            final Map<String, Predicate<AuditEventMetaInfo>> map = new HashMap<>();
            for (final String policyName : settings.getGroups(FILTER_POLICY_PREFIX, true).keySet()) {
                map.put(policyName, new EventFilterPolicy(policyName, settings).ignorePredicate());
            }
            policyMap = Collections.unmodifiableMap(map);
        }

        Predicate<AuditEventMetaInfo> ignorePredicate() {
            return policyMap.values().stream().reduce(x -> false, (x, y) -> x.or(y));
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
        AuditEventMetaInfo(Optional<User> user, Optional<String> realm, Optional<String[]> roles, Optional<String[]> indices) {
            this.principal = user.map(u -> u.principal()).orElse("");
            this.realm = realm.orElse("");
            // Supplier indirection and lazy generation of Streams serves 2 purposes:
            // 1. streams might not get generated due to short circuiting logical
            // conditions on the `principal` and `realm` fields
            // 2. reusability of the AuditEventMetaInfo instance: in this case Streams have
            // to be regenerated as they cannot be operated upon twice
            this.roles = () -> roles.filter(r -> r.length != 0).map(Arrays::stream).orElse(Stream.of(""));
            this.indices = () -> indices.filter(i -> i.length != 0).map(Arrays::stream).orElse(Stream.of(""));
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
        final DiscoveryNode localNode = localNodeInfo.localNode;
        if (localNode == null || localNode.equals(newLocalNode) == false) {
            // no need to synchronize, called only from the cluster state applier thread
            localNodeInfo = new LocalNodeInfo(settings, newLocalNode);
        }
    }

    static class LocalNodeInfo {
        private final DiscoveryNode localNode;
        private final String prefix;
        private final String localOriginTag;

        LocalNodeInfo(Settings settings, @Nullable DiscoveryNode newLocalNode) {
            this.localNode = newLocalNode;
            this.prefix = resolvePrefix(settings, newLocalNode);
            this.localOriginTag = localOriginTag(newLocalNode);
        }

        static String resolvePrefix(Settings settings, @Nullable DiscoveryNode localNode) {
            final StringBuilder builder = new StringBuilder();
            if (HOST_ADDRESS_SETTING.get(settings)) {
                final String address = localNode != null ? localNode.getHostAddress() : null;
                if (address != null) {
                    builder.append("[").append(address).append("] ");
                }
            }
            if (HOST_NAME_SETTING.get(settings)) {
                final String hostName = localNode != null ? localNode.getHostName() : null;
                if (hostName != null) {
                    builder.append("[").append(hostName).append("] ");
                }
            }
            if (NODE_NAME_SETTING.get(settings)) {
                final String name = settings.get("name");
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
            return new StringBuilder("origin_type=[local_node], origin_address=[").append(localNode.getHostAddress())
                    .append("]")
                    .toString();
        }
    }
}
