/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;

/**
 * Kerberos Realm - only SPNEGO mechanism is supported for authentication.
 */
public final class KerberosRealm extends Realm implements CachingRealm {
    public static final Oid SPNEGO_OID = getSpnegoOid();
    private static final Logger KRB_REALM_LOGGER = ESLoggerFactory.getLogger(KerberosRealm.class);

    private static Oid getSpnegoOid() {
        Oid oid = null;
        try {
            oid = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException gsse) {
            KRB_REALM_LOGGER.log(Level.FATAL, "Unable to create SPNEGO OID 1.3.6.1.5.5.2 !", gsse);
        }
        return oid;
    }

    private final Cache<String, User> userPrincipalNameToUserCache;
    private final NativeRoleMappingStore userRoleMapper;
    private KerberosTicketValidator kerberosTicketValidator;

    public KerberosRealm(final RealmConfig config, final NativeRoleMappingStore nativeRoleMappingStore) {
        super(KerberosRealmSettings.TYPE, config);
        this.userRoleMapper = nativeRoleMappingStore;
        this.userPrincipalNameToUserCache =
                CacheBuilder.<String, User>builder().setExpireAfterWrite(KerberosRealmSettings.CACHE_TTL_SETTING.get(config.settings()))
                        .setMaximumWeight(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.get(config.settings())).build();
        this.kerberosTicketValidator = new KerberosTicketValidator();
    }

    // pkg scoped for testing
    KerberosRealm(final RealmConfig config, final NativeRoleMappingStore nativeRoleMappingStore,
            final Cache<String, User> userPrincipalNameToUserCache, final KerberosTicketValidator kerberosTicketValidator) {
        super(KerberosRealmSettings.TYPE, config);
        this.userRoleMapper = nativeRoleMappingStore;
        this.userPrincipalNameToUserCache = userPrincipalNameToUserCache;
        this.kerberosTicketValidator = kerberosTicketValidator;
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
    public String getWWWAuthenticateHeaderValue() {
        return KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER;
    }

    @Override
    public AuthenticationToken token(final ThreadContext context) {
        return KerberosAuthenticationToken.extractToken(context);
    }

    @Override
    public void authenticate(final AuthenticationToken token, final ActionListener<AuthenticationResult> listener) {
        if (token instanceof KerberosAuthenticationToken) {
            final KerberosAuthenticationToken kerbAuthnToken = (KerberosAuthenticationToken) token;
            AuthenticationResult authenticationResult = AuthenticationResult.terminate("Spnego authentication failure", null);

            try {
                final Tuple<String, String> userPrincipalNameOutToken = kerberosTicketValidator.validateTicket("*",
                        GSSName.NT_HOSTBASED_SERVICE, (String) kerbAuthnToken.credentials(), config);

                if (userPrincipalNameOutToken != null) {
                    final String username = userPrincipalNameOutToken.v1();

                    User user = userPrincipalNameToUserCache.get(username);
                    if (user != null) {
                        listener.onResponse(AuthenticationResult.success(user));
                    } else {
                        final Map<String, Object> userMetadata = new HashMap<>();
                        // TODO how do we return this as Rest Response header "WWW-Authenticate: <token> ?"
                        userMetadata.put("_www-authenticate", userPrincipalNameOutToken.v2());
                        final UserRoleMapper.UserData userData =
                                new UserRoleMapper.UserData(username, null, Collections.emptySet(), null, this.config);
                        userRoleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
                            final User computedUser =
                                    new User(username, roles.toArray(new String[roles.size()]), null, null, userMetadata, true);
                            userPrincipalNameToUserCache.put(username, computedUser);
                            listener.onResponse(AuthenticationResult.success(computedUser));
                        }, listener::onFailure));

                        /**
                         * TODO: bizybot
                         * Lookup user in case of AD/LDAP realm present, returning fully
                         * populated User object.
                         */
                    }

                    return;
                } else {
                    authenticationResult = AuthenticationResult.terminate("Could not validate kerberos ticket", null);
                }
            } catch (LoginException e) {
                logger.log(Level.ERROR, "Exception occurred in service login", e);
                authenticationResult = AuthenticationResult.terminate("Internal server error: Exception occurred in service login", e);
            } catch (GSSException e) {
                logger.log(Level.ERROR, "Exception occurred in GSS-API", e);
                authenticationResult = AuthenticationResult.terminate("Internal server error: Exception occurred in GSS API", e);
            }

            listener.onResponse(authenticationResult);
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        /**
         * TODO: bizybot Support later
         */
        listener.onResponse(null);
    }

}
