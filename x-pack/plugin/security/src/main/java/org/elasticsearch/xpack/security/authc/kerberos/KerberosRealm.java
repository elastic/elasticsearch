/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTicketValidator;
import org.elasticsearch.xpack.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.ietf.jgss.GSSException;

import java.nio.file.Path;
import java.util.Collections;

import javax.security.auth.login.LoginException;

/**
 * Kerberos Realm - only SPNEGO mechanism is supported for authentication.
 */
public final class KerberosRealm extends Realm implements CachingRealm {

    private final Cache<String, User> userPrincipalNameToUserCache;
    private final NativeRoleMappingStore userRoleMapper;
    private final KerberosTicketValidator kerberosTicketValidator;
    private final ThreadPool threadPool;

    public KerberosRealm(final RealmConfig config, final NativeRoleMappingStore nativeRoleMappingStore, final ThreadPool threadPool) {
        this(config, nativeRoleMappingStore, new KerberosTicketValidator(), threadPool, null);
    }

    // pkg scoped for testing
    KerberosRealm(final RealmConfig config, final NativeRoleMappingStore nativeRoleMappingStore,
            final KerberosTicketValidator kerberosTicketValidator, final ThreadPool threadPool,
            final Cache<String, User> userPrincipalNameToUserCache) {
        super(KerberosRealmSettings.TYPE, config);
        this.userRoleMapper = nativeRoleMappingStore;
        this.userRoleMapper.refreshRealmOnChange(this);
        this.userPrincipalNameToUserCache = (userPrincipalNameToUserCache == null)
                ? CacheBuilder.<String, User>builder().setExpireAfterWrite(KerberosRealmSettings.CACHE_TTL_SETTING.get(config.settings()))
                        .setMaximumWeight(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.get(config.settings())).build()
                : userPrincipalNameToUserCache;
        this.kerberosTicketValidator = kerberosTicketValidator;
        this.threadPool = threadPool;
    }

    @Override
    public String getWWWAuthenticateHeaderValue() {
        return KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER.trim();
    }

    @Override
    public void expire(final String username) {
        userPrincipalNameToUserCache.invalidate(username);
    }

    @Override
    public void expireAll() {
        userPrincipalNameToUserCache.invalidateAll();
    }

    @Override
    public boolean supports(final AuthenticationToken token) {
        return token instanceof KerberosAuthenticationToken;
    }

    @Override
    public AuthenticationToken token(final ThreadContext context) {
        return KerberosAuthenticationToken.extractToken(context.getHeader(KerberosAuthenticationToken.AUTH_HEADER));
    }

    @Override
    public void authenticate(final AuthenticationToken token, final ActionListener<AuthenticationResult> listener) {
        if (token instanceof KerberosAuthenticationToken) {
            final KerberosAuthenticationToken kerbAuthnToken = (KerberosAuthenticationToken) token;
            AuthenticationResult authenticationResult = AuthenticationResult.terminate("Spnego authentication failure", null);

            try {
                final Path keytabPath =
                        config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));
                final boolean krbDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
                final Tuple<String, String> userPrincipalNameOutToken =
                        kerberosTicketValidator.validateTicket((byte[]) kerbAuthnToken.credentials(), keytabPath, krbDebug);

                if (userPrincipalNameOutToken != null && userPrincipalNameOutToken.v1() != null) {
                    userDetails(userPrincipalNameOutToken, listener);
                    return;
                } else {
                    String errorMessage = "failed to authenticate user, invalid kerberos ticket";
                    String outToken = "";
                    if (userPrincipalNameOutToken != null) {
                        // Ongoing sec context establishment, return UNAUTHORIZED with out token if any
                        outToken = userPrincipalNameOutToken.v2();
                        errorMessage = "failed to authenticate user, gss netogiation not complete";
                    }
                    addResponseHeaderToThreadContext(outToken);
                    authenticationResult = AuthenticationResult.terminate(errorMessage, null);
                }
            } catch (LoginException e) {
                logger.log(Level.ERROR, "Exception occurred in service login", e);
                authenticationResult = AuthenticationResult.terminate("failed to authenticate user, service login failure",
                        KerberosAuthenticationToken.unauthorized(e.getLocalizedMessage(), e, (Object[]) null));
            } catch (GSSException e) {
                logger.log(Level.ERROR, "Exception occurred in GSS-API", e);
                authenticationResult = AuthenticationResult.terminate("failed to authenticate user, GSS negotiation failure",
                        KerberosAuthenticationToken.unauthorized(e.getLocalizedMessage(), e, (Object[]) null));
            }

            listener.onResponse(authenticationResult);
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    private void userDetails(final Tuple<String, String> userPrincipalNameOutToken, final ActionListener<AuthenticationResult> listener) {
        // Add token if any to response header
        addResponseHeaderToThreadContext(userPrincipalNameOutToken.v2());

        final String username = userPrincipalNameOutToken.v1();
        User user = userPrincipalNameToUserCache.get(username);
        if (user != null) {
            listener.onResponse(AuthenticationResult.success(user));
        } else {
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(username, null, Collections.emptySet(), null, this.config);
            userRoleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
                final User computedUser = new User(username, roles.toArray(new String[roles.size()]), null, null, null, true);
                userPrincipalNameToUserCache.put(username, computedUser);
                listener.onResponse(AuthenticationResult.success(computedUser));
            }, listener::onFailure));

            /**
             * TODO: bizybot AD/LDAP user lookup if lookup realm configured
             */
        }
    }

    private void addResponseHeaderToThreadContext(String outToken) {
        threadPool.getThreadContext().addResponseHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE,
                KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + outToken);
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        // TODO bizybot support later
        listener.onResponse(null);
    }

}
