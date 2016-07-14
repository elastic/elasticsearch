/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authz.privilege.SystemPrivilege;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessage;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.elasticsearch.xpack.security.audit.AuditUtil.indices;
import static org.elasticsearch.xpack.security.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.xpack.security.Security.setting;

/**
 *
 */
public class LoggingAuditTrail extends AbstractComponent implements AuditTrail {

    public static final String NAME = "logfile";
    public static final Setting<Boolean> HOST_ADDRESS_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_host_address"), false, Property.NodeScope);
    public static final Setting<Boolean> HOST_NAME_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_host_name"), false, Property.NodeScope);
    public static final Setting<Boolean> NODE_NAME_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_name"), true, Property.NodeScope);

    private final ESLogger logger;
    private final ClusterService clusterService;
    private final ThreadContext threadContext;

    private String prefix;

    @Override
    public String name() {
        return NAME;
    }

    @Inject
    public LoggingAuditTrail(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        this(settings, clusterService, Loggers.getLogger(LoggingAuditTrail.class), threadPool.getThreadContext());
    }

    LoggingAuditTrail(Settings settings, ClusterService clusterService, ESLogger logger, ThreadContext threadContext) {
        super(settings);
        this.logger = logger;
        this.clusterService = clusterService;
        this.threadContext = threadContext;
    }

    private String getPrefix() {
        if (prefix == null) {
            prefix = resolvePrefix(settings, clusterService.localNode());
        }
        return prefix;
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.warn("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [anonymous_access_denied]\t{}, action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, message.getClass().getSimpleName());
            } else {
                logger.warn("{}[transport] [anonymous_access_denied]\t{}, action=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [anonymous_access_denied]\t{}, uri=[{}], request_body=[{}]", getPrefix(),
                hostAttributes(request), request.uri(), restRequestContent(request));
        } else {
            logger.warn("{}[rest] [anonymous_access_denied]\t{}, uri=[{}]", getPrefix(), hostAttributes(request), request.uri());
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}], request=[{}]",
                        getPrefix(), originAttributes(message, clusterService.localNode(), threadContext), token.principal(),
                                                 action, indices, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), token.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), token.principal(), action,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), token.principal(), action);
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [authentication_failed]\t{}, uri=[{}], request_body=[{}]", getPrefix(), hostAttributes(request),
                request.uri(), restRequestContent(request));
        } else {
            logger.error("{}[rest] [authentication_failed]\t{}, uri=[{}]", getPrefix(), hostAttributes(request), request.uri());
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, action=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}], request_body=[{}]", getPrefix(),
                hostAttributes(request), token.principal(), request.uri(), restRequestContent(request));
        } else {
            logger.error("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}]", getPrefix(), hostAttributes(request),
                token.principal(), request.uri());
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (logger.isTraceEnabled()) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.trace("{}[transport] [authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], indices=[{}], " +
                    "request=[{}]", getPrefix(), realm, originAttributes(message, clusterService.localNode(), threadContext),
                    token.principal(), action, indices, message.getClass().getSimpleName());
            } else {
                logger.trace("{}[transport] [authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], request=[{}]",
                    getPrefix(), realm, originAttributes(message, clusterService.localNode(), threadContext), token.principal(),
                    action, message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}[rest] [authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}], request_body=[{}]", getPrefix(),
                realm, hostAttributes(request), token.principal(), request.uri(), restRequestContent(request));
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage message) {
        String indices = indicesString(message);

        // special treatment for internal system actions - only log on trace
        if ((SystemUser.is(user) && SystemPrivilege.INSTANCE.predicate().test(action)) || XPackUser.is(user)) {
            if (logger.isTraceEnabled()) {
                if (indices != null) {
                    logger.trace("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices,
                            message.getClass().getSimpleName());
                } else {
                    logger.trace("{}[transport] [access_granted]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                        originAttributes(message, clusterService.localNode(), threadContext), principal(user), action,
                            message.getClass().getSimpleName());
                }
            }
            return;
        }

        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_granted]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [access_granted]\t{}, {}, action=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_denied]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [access_denied]\t{}, {}, action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_denied]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [access_denied]\t{}, {}, action=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), principal(user), action);
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [tampered_request]\t{}, uri=[{}], request_body=[{}]", getPrefix(), hostAttributes(request),
                request.uri(), restRequestContent(request));
        } else {
            logger.error("{}[rest] [tampered_request]\t{}, uri=[{}]", getPrefix(), hostAttributes(request), request.uri());
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, action=[{}], request=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, action=[{}]", getPrefix(),
                    originAttributes(message, clusterService.localNode(), threadContext), action);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        String indices = indicesString(request);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}], request=[{}]", getPrefix(),
                    originAttributes(request, clusterService.localNode(), threadContext), principal(user), action, indices,
                        request.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}]", getPrefix(),
                    originAttributes(request, clusterService.localNode(), threadContext), principal(user), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, {}, action=[{}], request=[{}]", getPrefix(),
                    originAttributes(request, clusterService.localNode(), threadContext), principal(user), action,
                        request.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, {}, action=[{}]", getPrefix(),
                    originAttributes(request, clusterService.localNode(), threadContext), principal(user), action);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}[ip_filter] [connection_granted]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", getPrefix(),
                    NetworkAddress.format(inetAddress), profile, rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, SecurityIpFilterRule rule) {
        logger.error("{}[ip_filter] [connection_denied]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", getPrefix(),
                NetworkAddress.format(inetAddress), profile, rule);
    }

    @Override
    public void runAsGranted(User user, String action, TransportMessage message) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], action=[{}], request=[{}]",
                getPrefix(), originAttributes(message, clusterService.localNode(), threadContext), user.principal(),
                    user.runAs().principal(), action, message.getClass().getSimpleName());
        } else {
            logger.info("{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], action=[{}]", getPrefix(),
                originAttributes(message, clusterService.localNode(), threadContext), user.principal(),
                    user.runAs().principal(), action);
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], action=[{}], request=[{}]",
                getPrefix(), originAttributes(message, clusterService.localNode(), threadContext), user.principal(),
                    user.runAs().principal(), action, message.getClass().getSimpleName());
        } else {
            logger.info("{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], action=[{}]", getPrefix(),
                originAttributes(message, clusterService.localNode(), threadContext), user.principal(),
                    user.runAs().principal(), action);
        }
    }

    @Override
    public void runAsDenied(User user, RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [run_as_denied]\t{}, principal=[{}], uri=[{}], request_body=[{}]", getPrefix(),
                    hostAttributes(request), user.principal(), request.uri(), restRequestContent(request));
        } else {
            logger.info("{}[transport] [run_as_denied]\t{}, principal=[{}], uri=[{}]", getPrefix(),
                    hostAttributes(request), user.principal(), request.uri());
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
            if (address instanceof InetSocketTransportAddress) {
                builder.append("origin_address=[").
                        append(NetworkAddress.format(((InetSocketTransportAddress) address).address().getAddress())).
                        append("]");
            } else {
                builder.append("origin_address=[").append(address).append("]");
            }
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
        String[] indices = indices(message);
        return indices == null ? null : arrayToCommaDelimitedString(indices);
    }

    static String principal(User user) {
        StringBuilder builder = new StringBuilder("principal=[");
        if (user.runAs() != null) {
            builder.append(user.runAs().principal()).append("], run_by_principal=[");
        }
        return builder.append(user.principal()).append("]").toString();
    }

    public static void registerSettings(List<Setting<?>> settings) {
        settings.add(HOST_ADDRESS_SETTING);
        settings.add(HOST_NAME_SETTING);
        settings.add(NODE_NAME_SETTING);
    }
}
