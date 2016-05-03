/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

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
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.elasticsearch.shield.audit.AuditUtil.indices;
import static org.elasticsearch.shield.audit.AuditUtil.restRequestContent;
import static org.elasticsearch.shield.Security.setting;

/**
 *
 */
public class LoggingAuditTrail extends AbstractLifecycleComponent<LoggingAuditTrail> implements AuditTrail {

    public static final String NAME = "logfile";
    public static final Setting<Boolean> HOST_ADDRESS_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_host_address"), false, Property.NodeScope);
    public static final Setting<Boolean> HOST_NAME_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_host_name"), false, Property.NodeScope);
    public static final Setting<Boolean> NODE_NAME_SETTING =
            Setting.boolSetting(setting("audit.logfile.prefix.emit_node_name"), true, Property.NodeScope);

    private final ESLogger logger;
    private final Transport transport;
    private final ThreadContext threadContext;

    private String prefix;

    @Override
    public String name() {
        return NAME;
    }

    @Inject
    public LoggingAuditTrail(Settings settings, Transport transport, ThreadPool threadPool) {
        this(settings, transport, Loggers.getLogger(LoggingAuditTrail.class), threadPool.getThreadContext());
    }

    LoggingAuditTrail(Settings settings, Transport transport, ESLogger logger, ThreadContext threadContext) {
        this("", settings, transport, logger, threadContext);
    }

    LoggingAuditTrail(String prefix, Settings settings, Transport transport, ESLogger logger, ThreadContext threadContext) {
        super(settings);
        this.logger = logger;
        this.prefix = prefix;
        this.transport = transport;
        this.threadContext = threadContext;
    }


    @Override
    protected void doStart() {
        if (transport.lifecycleState() == Lifecycle.State.STARTED) {
            prefix = resolvePrefix(settings, transport);
        } else {
            transport.addLifecycleListener(new LifecycleListener() {
                @Override
                public void afterStart() {
                    prefix = resolvePrefix(settings, transport);
                }
            });
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void anonymousAccessDenied(String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), action, indices, message.getClass().getSimpleName());
            } else {
                logger.warn("{}[transport] [anonymous_access_denied]\t{}, action=[{}], indices=[{}]", prefix, originAttributes(message,
                        transport, threadContext), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [anonymous_access_denied]\t{}, action=[{}], request=[{}]", prefix, originAttributes(message,
                        transport, threadContext), action, message.getClass().getSimpleName());
            } else {
                logger.warn("{}[transport] [anonymous_access_denied]\t{}, action=[{}]", prefix, originAttributes(message, transport,
                        threadContext), action);
            }
        }
    }

    @Override
    public void anonymousAccessDenied(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [anonymous_access_denied]\t{}, uri=[{}], request_body=[{}]", prefix, hostAttributes(request), request
                    .uri(), restRequestContent(request));
        } else {
            logger.warn("{}[rest] [anonymous_access_denied]\t{}, uri=[{}]", prefix, hostAttributes(request), request.uri());
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}], request=[{}]",
                        prefix, originAttributes(message, transport, threadContext), token.principal(), action, indices, message.getClass
                                ().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], indices=[{}]", prefix,
                        originAttributes(message, transport, threadContext), token.principal(), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), token.principal(), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, principal=[{}], action=[{}]", prefix, originAttributes(message,
                        transport, threadContext), token.principal(), action);
            }
        }
    }

    @Override
    public void authenticationFailed(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [authentication_failed]\t{}, uri=[{}], request_body=[{}]", prefix, hostAttributes(request), request
                    .uri(), restRequestContent(request));
        } else {
            logger.error("{}[rest] [authentication_failed]\t{}, uri=[{}]", prefix, hostAttributes(request), request.uri());
        }
    }

    @Override
    public void authenticationFailed(String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), action, indices, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, action=[{}], indices=[{}]", prefix, originAttributes(message,
                        transport, threadContext), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [authentication_failed]\t{}, action=[{}], request=[{}]", prefix, originAttributes(message,
                        transport, threadContext), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [authentication_failed]\t{}, action=[{}]", prefix, originAttributes(message, transport,
                        threadContext), action);
            }
        }
    }

    @Override
    public void authenticationFailed(AuthenticationToken token, RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}], request_body=[{}]", prefix, hostAttributes
                    (request), token.principal(), request.uri(), restRequestContent(request));
        } else {
            logger.error("{}[rest] [authentication_failed]\t{}, principal=[{}], uri=[{}]", prefix, hostAttributes(request), token
                    .principal(), request.uri());
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, String action, TransportMessage message) {
        if (logger.isTraceEnabled()) {
            String indices = indicesString(message);
            if (indices != null) {
                logger.trace("{}[transport] [authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], indices=[{}], " +
                        "request=[{}]", prefix, realm, originAttributes(message, transport, threadContext), token.principal(), action,
                        indices, message.getClass().getSimpleName());
            } else {
                logger.trace("{}[transport] [authentication_failed]\trealm=[{}], {}, principal=[{}], action=[{}], request=[{}]", prefix,
                        realm, originAttributes(message, transport, threadContext), token.principal(), action, message.getClass()
                                .getSimpleName());
            }
        }
    }

    @Override
    public void authenticationFailed(String realm, AuthenticationToken token, RestRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}[rest] [authentication_failed]\trealm=[{}], {}, principal=[{}], uri=[{}], request_body=[{}]", prefix, realm,
                    hostAttributes(request), token.principal(), request.uri(), restRequestContent(request));
        }
    }

    @Override
    public void accessGranted(User user, String action, TransportMessage message) {
        String indices = indicesString(message);

        // special treatment for internal system actions - only log on trace
        if ((SystemUser.is(user) && SystemPrivilege.INSTANCE.predicate().test(action)) || XPackUser.is(user)) {
            if (logger.isTraceEnabled()) {
                if (indices != null) {
                    logger.trace("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}], request=[{}]", prefix,
                            originAttributes(message, transport, threadContext), principal(user), action, indices,
                            message.getClass().getSimpleName());
                } else {
                    logger.trace("{}[transport] [access_granted]\t{}, {}, action=[{}], request=[{}]", prefix,
                            originAttributes(message, transport, threadContext), principal(user), action,
                            message.getClass().getSimpleName());
                }
            }
            return;
        }

        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [access_granted]\t{}, {}, action=[{}], indices=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_granted]\t{}, {}, action=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action, message.getClass().getSimpleName());
            } else {
                logger.info("{}[transport] [access_granted]\t{}, {}, action=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action);
            }
        }
    }

    @Override
    public void accessDenied(User user, String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_denied]\t{}, {}, action=[{}], indices=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action, indices,
                        message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [access_denied]\t{}, {}, action=[{}], indices=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [access_denied]\t{}, {}, action=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [access_denied]\t{}, {}, action=[{}]", prefix,
                        originAttributes(message, transport, threadContext), principal(user), action);
            }
        }
    }

    @Override
    public void tamperedRequest(RestRequest request) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[rest] [tampered_request]\t{}, uri=[{}], request_body=[{}]", prefix, hostAttributes(request), request.uri(),
                    restRequestContent(request));
        } else {
            logger.error("{}[rest] [tampered_request]\t{}, uri=[{}]", prefix, hostAttributes(request), request.uri());
        }
    }

    @Override
    public void tamperedRequest(String action, TransportMessage message) {
        String indices = indicesString(message);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), action, indices, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, action=[{}], indices=[{}]", prefix,
                        originAttributes(message, transport, threadContext), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, action=[{}], request=[{}]", prefix,
                        originAttributes(message, transport, threadContext), action, message.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, action=[{}]", prefix,
                        originAttributes(message, transport, threadContext), action);
            }
        }
    }

    @Override
    public void tamperedRequest(User user, String action, TransportMessage request) {
        String indices = indicesString(request);
        if (indices != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}], request=[{}]", prefix,
                        originAttributes(request, transport, threadContext), principal(user), action, indices,
                        request.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, {}, action=[{}], indices=[{}]", prefix,
                        originAttributes(request, transport, threadContext), principal(user), action, indices);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("{}[transport] [tampered_request]\t{}, {}, action=[{}], request=[{}]", prefix,
                        originAttributes(request, transport, threadContext), principal(user), action, request.getClass().getSimpleName());
            } else {
                logger.error("{}[transport] [tampered_request]\t{}, {}, action=[{}]", prefix,
                        originAttributes(request, transport, threadContext), principal(user), action);
            }
        }
    }

    @Override
    public void connectionGranted(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}[ip_filter] [connection_granted]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", prefix,
                    NetworkAddress.format(inetAddress), profile, rule);
        }
    }

    @Override
    public void connectionDenied(InetAddress inetAddress, String profile, ShieldIpFilterRule rule) {
        logger.error("{}[ip_filter] [connection_denied]\torigin_address=[{}], transport_profile=[{}], rule=[{}]", prefix,
                NetworkAddress.format(inetAddress), profile, rule);
    }

    @Override
    public void runAsGranted(User user, String action, TransportMessage message) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], action=[{}], request=[{}]", prefix,
                    originAttributes(message, transport, threadContext), user.principal(), user.runAs().principal(), action,
                    message.getClass().getSimpleName());
        } else {
            logger.info("{}[transport] [run_as_granted]\t{}, principal=[{}], run_as_principal=[{}], action=[{}]", prefix,
                    originAttributes(message, transport, threadContext), user.principal(), user.runAs().principal(), action);
        }
    }

    @Override
    public void runAsDenied(User user, String action, TransportMessage message) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], action=[{}], request=[{}]", prefix,
                    originAttributes(message, transport, threadContext), user.principal(), user.runAs().principal(), action,
                    message.getClass().getSimpleName());
        } else {
            logger.info("{}[transport] [run_as_denied]\t{}, principal=[{}], run_as_principal=[{}], action=[{}]", prefix,
                    originAttributes(message, transport, threadContext), user.principal(), user.runAs().principal(), action);
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

    static String originAttributes(TransportMessage message, Transport transport, ThreadContext threadContext) {
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
                .append(transport.boundAddress().publishAddress().getAddress())
                .append("]")
                .toString();
    }

    static String resolvePrefix(Settings settings, Transport transport) {
        StringBuilder builder = new StringBuilder();
        if (HOST_ADDRESS_SETTING.get(settings)) {
            String address = transport.boundAddress().publishAddress().getAddress();
            if (address != null) {
                builder.append("[").append(address).append("] ");
            }
        }
        if (HOST_NAME_SETTING.get(settings)) {
            String hostName = transport.boundAddress().publishAddress().getHost();
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

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(HOST_ADDRESS_SETTING);
        settingsModule.registerSetting(HOST_NAME_SETTING);
        settingsModule.registerSetting(NODE_NAME_SETTING);
    }
}
