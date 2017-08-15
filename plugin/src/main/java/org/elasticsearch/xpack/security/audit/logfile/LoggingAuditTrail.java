/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authz.privilege.SystemPrivilege;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.audit.AuditLevel.ACCESS_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.ACCESS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.ANONYMOUS_ACCESS_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.AUTHENTICATION_FAILED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.REALM_AUTHENTICATION_FAILED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.CONNECTION_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.CONNECTION_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.RUN_AS_DENIED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.RUN_AS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.SYSTEM_ACCESS_GRANTED;
import static org.elasticsearch.xpack.security.audit.AuditLevel.TAMPERED_REQUEST;
import static org.elasticsearch.xpack.security.audit.AuditLevel.AUTHENTICATION_SUCCESS;
import static org.elasticsearch.xpack.security.audit.AuditLevel.parse;
import static org.elasticsearch.xpack.security.audit.AuditUtil.indices;
import static org.elasticsearch.xpack.security.audit.AuditUtil.restRequestContent;

public class LoggingAuditTrail extends AbstractComponent implements AuditTrail {

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

    private final Logger logger;
    private final ClusterService clusterService;
    private final ThreadContext threadContext;
    private final EnumSet<AuditLevel> events;
    private final boolean includeRequestBody;

    private String prefix;

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
        this.clusterService = clusterService;
        this.threadContext = threadContext;
        this.events = parse(INCLUDE_EVENT_SETTINGS.get(settings), EXCLUDE_EVENT_SETTINGS.get(settings));
        this.includeRequestBody = INCLUDE_REQUEST_BODY.get(settings);
    }

    private String getPrefix() {
        if (prefix == null) {
            prefix = resolvePrefix(settings, clusterService.localNode());
        }
        return prefix;
    }

    @Override
    public void authenticationSuccess(String realm, User user, RestRequest request) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_success]\t{}, realm=[{}], uri=[{}], params=[{}], request_body=[{}]", getPrefix(),
                        principal(user), realm, request.uri(), request.params(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_success]\t{}, realm=[{}], uri=[{}], params=[{}]", getPrefix(), principal(user), realm,
                        request.uri(), request.params());
            }
        }
    }

    @Override
    public void authenticationSuccess(String realm, User user, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_SUCCESS)) {
            logger.info("{}[transport] [authentication_success]\t{}, {}, realm=[{}], action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), realm, action,
                    message.getClass().getSimpleName());
        }
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.info("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [anonymous_access_denied]\t{}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), action, message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (events.contains(ANONYMOUS_ACCESS_DENIED)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [anonymous_access_denied]\t{}, uri=[{}], request_body=[{}]", getPrefix(),
                        hostAttributes(request), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [anonymous_access_denied]\t{}, uri=[{}]", getPrefix(), hostAttributes(request), request.uri());
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.info("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}], request=[{}]",
                        getPrefix(), originAttributes(message, clusterService.localNode(), threadContext), token.principal(),
                        action, indices, message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), token.principal(), action,
                        message.getClass().getSimpleName());
            }

        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_failed]\t{}, uri=[{}], request_body=[{}]", getPrefix(), hostAttributes(request),
                        request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_failed]\t{}, uri=[{}]", getPrefix(), hostAttributes(request), request.uri());
            }
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage message) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.info("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [authentication_failed]\t{}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), action, message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (events.contains(AUTHENTICATION_FAILED)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}], request_body=[{}]", getPrefix(),
                        hostAttributes(request), token.principal(), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}]", getPrefix(), hostAttributes(request),
                        token.principal(), request.uri());
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.info("{}[transport] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], indices=[{}], " +
                                "request=[{}]", getPrefix(), realm, originAttributes(message, clusterService.localNode(), threadContext),
                        token.principal(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], request=[{}]",
                        getPrefix(), realm, originAttributes(message, clusterService.localNode(), threadContext), token.principal(),
                        action, message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (events.contains(REALM_AUTHENTICATION_FAILED)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}], request_body=[{}]",
                        getPrefix(), realm, hostAttributes(request), token.principal(), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [realm_authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}]", getPrefix(),
                        realm, hostAttributes(request), token.principal(), request.uri());
            }
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage message, @Nullable Set<String> specificIndices) {
        final boolean isSystem = (SystemUser.is(user) && SystemPrivilege.INSTANCE.predicate().test(action)) || XPackUser.is(user);
        final boolean logSystemAccessGranted = isSystem && events.contains(SYSTEM_ACCESS_GRANTED);
        final boolean shouldLog = logSystemAccessGranted || (isSystem == false && events.contains(ACCESS_GRANTED));
        if (shouldLog) {
            String indices = specificIndices == null ? indicesString(message) : collectionToCommaDelimitedString(specificIndices);
            if (indices != null) {
                logger.info("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [access_granted]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), principal(user), action,
                        message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message, @Nullable Set<String> specificIndices) {
        if (events.contains(ACCESS_DENIED)) {
            String indices = specificIndices == null ? indicesString(message) : collectionToCommaDelimitedString(specificIndices);
            if (indices != null) {
                logger.info("{}[transport] [access_denied]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [access_denied]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), principal(user), action,
                        message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        if (events.contains(TAMPERED_REQUEST)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [tampered_request]\t{}, uri=[{}], request_body=[{}]", getPrefix(), hostAttributes(request),
                        request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [tampered_request]\t{}, uri=[{}]", getPrefix(), hostAttributes(request), request.uri());
            }
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        if (events.contains(TAMPERED_REQUEST)) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.info("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [tampered_request]\t{}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), action,
                        message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        if (events.contains(TAMPERED_REQUEST)) {
            String indices = indicesString(request);
            if (indices != null) {
                logger.info("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(request, clusterService.localNode(), threadContext), principal(user), action, indices,
                        request.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [tampered_request]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(request, clusterService.localNode(), threadContext), principal(user), action,
                        request.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_GRANTED)) {
            logger.info("{}[ip_filter] [connection_granted]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", getPrefix(),
                    NetworkAddress.format(inetAddress), profile, rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (events.contains(CONNECTION_DENIED)) {
            logger.info("{}[ip_filter] [connection_denied]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", getPrefix(),
                    NetworkAddress.format(inetAddress), profile, rule);
        }
    }

    @Override
    public void runAsGranted(User user, String action, TransportMessage message) {
        if (events.contains(RUN_AS_GRANTED)) {
            logger.info("{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], action=[{}], request=[{}]",
                getPrefix(), originAttributes(message, clusterService.localNode(), threadContext), user.authenticatedUser().principal(),
                    user.principal(), action, message.getClass().getSimpleName());
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (events.contains(RUN_AS_DENIED)) {
            logger.info("{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], action=[{}], request=[{}]",
                getPrefix(), originAttributes(message, clusterService.localNode(), threadContext), user.authenticatedUser().principal(),
                    user.principal(), action, message.getClass().getSimpleName());
        }
    }

    @Override
    public void runAsDenied(User user, RestRequest request) {
        if (events.contains(RUN_AS_DENIED)) {
            if (includeRequestBody) {
                logger.info("{}[rest] [run_as_denied]\t{}, principal=[{}], uri=[{}], request_body=[{}]", getPrefix(),
                    hostAttributes(request), user.principal(), request.uri(), restRequestContent(request));
            } else {
                logger.info("{}[rest] [run_as_denied]\t{}, principal=[{}], uri=[{}]", getPrefix(),
                        hostAttributes(request), user.principal(), request.uri());
            }
        }
    }

    private static String hostAttributes(RestRequest request) {
        String formattedAddress;
        SocketAddress socketAddress = request.getRemoteAddress();
        if (socketAddress instanceof InetSocketAddress) {
            formattedAddress = NetworkAddress.format(((InetSocketAddress) socketAddress).getAddress());
        } else {
            formattedAddress = socketAddress.toString();
        }
        return "origin_address=[" + formattedAddress + "]";
    }

    static String originAttributes(TransportMessage message, DiscoveryNode localNode, ThreadContext threadContext) {
        StringBuilder builder = new StringBuilder();

        // first checking if the message originated in a rest call
        InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress != null) {
            builder.append("origin_type=[rest], origin_address=[").
                    append(NetworkAddress.format(restAddress.getAddress())).
                    append("]");
            return builder.toString();
        }

        // we'll see if was originated in a remote node
        TransportAddress address = message.remoteAddress();
        if (address != null) {
            builder.append("origin_type=[transport], ");
            builder.append("origin_address=[").
                        append(NetworkAddress.format(address.address().getAddress())).
                        append("]");
            return builder.toString();
        }

        // the call was originated locally on this node
        return builder.append("origin_type=[local_node], origin_address=[")
                .append(localNode.getHostAddress())
                .append("]")
                .toString();
    }

    static String resolvePrefix(Settings settings, DiscoveryNode localNode) {
        StringBuilder builder = new StringBuilder();
        if (HOST_ADDRESS_SETTING.get(settings)) {
            String address = localNode.getHostAddress();
            if (address != null) {
                builder.append("[").append(address).append("] ");
            }
        }
        if (HOST_NAME_SETTING.get(settings)) {
            String hostName = localNode.getHostName();
            if (hostName != null) {
                builder.append("[").append(hostName).append("] ");
            }
        }
        if (NODE_NAME_SETTING.get(settings)) {
            String name = settings.get("name");
            if (name != null) {
                builder.append("[").append(name).append("] ");
            }
        }
        return builder.toString();
    }

    static String indicesString(TransportMessage message) {
        Set<String> indices = indices(message);
        return indices == null ? null : collectionToCommaDelimitedString(indices);
    }

    static String principal(User user) {
        StringBuilder builder = new StringBuilder("principal=[");
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
    }
}
