/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.ietf.jgss.GSSException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginException;

import static org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken.AUTH_HEADER;
import static org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX;
import static org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken.NEGOTIATE_SCHEME_NAME;
import static org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken.WWW_AUTHENTICATE;
import static org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken.unauthorized;
import static org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken.unauthorizedWithOutputToken;

/**
 * This class provides support for Kerberos authentication using spnego
 * mechanism.
 * <p>
 * It provides support to extract kerberos ticket using
 * {@link KerberosAuthenticationToken#extractToken(String)} to build
 * {@link KerberosAuthenticationToken} and then authenticating user when
 * {@link KerberosTicketValidator} validates the ticket.
 * <p>
 * On successful authentication, it will build {@link User} object populated
 * with roles and will return {@link AuthenticationResult} with user object. On
 * authentication failure, it will return {@link AuthenticationResult} with
 * status to terminate authentication process.
 */
public final class KerberosRealm extends Realm implements CachingRealm {

    private final Cache<String, User> userPrincipalNameToUserCache;
    private final NativeRoleMappingStore userRoleMapper;
    private final KerberosTicketValidator kerberosTicketValidator;
    private final ThreadPool threadPool;
    private final Path keytabPath;
    private final boolean enableKerberosDebug;
    private final boolean removeRealmName;

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
        final TimeValue ttl = KerberosRealmSettings.CACHE_TTL_SETTING.get(config.settings());
        if (ttl.getNanos() > 0) {
            this.userPrincipalNameToUserCache = (userPrincipalNameToUserCache == null)
                    ? CacheBuilder.<String, User>builder()
                            .setExpireAfterWrite(KerberosRealmSettings.CACHE_TTL_SETTING.get(config.settings()))
                            .setMaximumWeight(KerberosRealmSettings.CACHE_MAX_USERS_SETTING.get(config.settings())).build()
                    : userPrincipalNameToUserCache;
        } else {
            this.userPrincipalNameToUserCache = null;
        }
        this.kerberosTicketValidator = kerberosTicketValidator;
        this.threadPool = threadPool;
        this.keytabPath = config.env().configFile().resolve(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH.get(config.settings()));

        if (Files.exists(keytabPath) == false) {
            throw new IllegalArgumentException("configured service key tab file [" + keytabPath + "] does not exist");
        }
        if (Files.isDirectory(keytabPath)) {
            throw new IllegalArgumentException("configured service key tab file [" + keytabPath + "] is a directory");
        }
        if (Files.isReadable(keytabPath) == false) {
            throw new IllegalArgumentException("configured service key tab file [" + keytabPath + "] must have read permission");
        }
        this.enableKerberosDebug = KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE.get(config.settings());
        this.removeRealmName = KerberosRealmSettings.SETTING_REMOVE_REALM_NAME.get(config.settings());
    }

    @Override
    public Map<String, List<String>> getAuthenticationFailureHeaders() {
        return Collections.singletonMap(WWW_AUTHENTICATE, Collections.singletonList(NEGOTIATE_SCHEME_NAME));
    }

    @Override
    public void expire(final String username) {
        if (userPrincipalNameToUserCache != null) {
            userPrincipalNameToUserCache.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        if (userPrincipalNameToUserCache != null) {
            userPrincipalNameToUserCache.invalidateAll();
        }
    }

    @Override
    public boolean supports(final AuthenticationToken token) {
        return token instanceof KerberosAuthenticationToken;
    }

    @Override
    public AuthenticationToken token(final ThreadContext context) {
        return KerberosAuthenticationToken.extractToken(context.getHeader(AUTH_HEADER));
    }

    @Override
    public void authenticate(final AuthenticationToken token, final ActionListener<AuthenticationResult> listener) {
        assert token instanceof KerberosAuthenticationToken;
        final KerberosAuthenticationToken kerbAuthnToken = (KerberosAuthenticationToken) token;
        kerberosTicketValidator.validateTicket((byte[]) kerbAuthnToken.credentials(), keytabPath, enableKerberosDebug,
                ActionListener.wrap(userPrincipalNameOutToken -> {
                    if (userPrincipalNameOutToken.v1() != null) {
                        final String username = maybeRemoveRealmName(userPrincipalNameOutToken.v1());
                        buildUser(username, userPrincipalNameOutToken.v2(), listener);
                    } else {
                        /**
                         * This is when security context could not be established may be due to ongoing
                         * negotiation and requires token to be sent back to peer for continuing
                         * further. We are terminating the authentication process as this is spengo
                         * negotiation and no other realm can handle this. We can have only one Kerberos
                         * realm in the system so terminating with RestStatus Unauthorized (401) and
                         * with 'WWW-Authenticate' header populated with value with token in the form
                         * 'Negotiate oYH1MIHyoAMK...'
                         */
                        String errorMessage = "failed to authenticate user, gss context negotiation not complete";
                        ElasticsearchSecurityException ese = unauthorized(errorMessage, null);
                        ese = unauthorizedWithOutputToken(ese, userPrincipalNameOutToken.v2());
                        listener.onResponse(AuthenticationResult.terminate(errorMessage, ese));
                    }
                }, e -> handleException(e, listener)));
    }

    /**
     * Usually principal names are in the form 'user/instance@REALM'. This method
     * removes '@REALM' part from the principal name if
     * {@link KerberosRealmSettings#SETTING_REMOVE_REALM_NAME} is {@code true} else
     * will return the input string.
     *
     * @param principalName user principal name
     * @return username after removal of realm
     */
    protected String maybeRemoveRealmName(final String principalName) {
        if (this.removeRealmName) {
            int foundAtIndex = principalName.indexOf('@');
            if (foundAtIndex > 0) {
                return principalName.substring(0, foundAtIndex);
            }
        }
        return principalName;
    }

    private void handleException(Exception e, final ActionListener<AuthenticationResult> listener) {
        if (e instanceof LoginException) {
            listener.onResponse(AuthenticationResult.terminate("failed to authenticate user, service login failure",
                    unauthorized(e.getLocalizedMessage(), e)));
        } else if (e instanceof GSSException) {
            listener.onResponse(AuthenticationResult.terminate("failed to authenticate user, gss context negotiation failure",
                    unauthorized(e.getLocalizedMessage(), e)));
        } else {
            listener.onFailure(e);
        }
    }

    private void buildUser(final String username, final String outToken, final ActionListener<AuthenticationResult> listener) {
        // if outToken is present then it needs to be communicated with peer, add it to
        // response header in thread context.
        if (Strings.hasText(outToken)) {
            threadPool.getThreadContext().addResponseHeader(WWW_AUTHENTICATE, NEGOTIATE_AUTH_HEADER_PREFIX + outToken);
        }
        final User user = (userPrincipalNameToUserCache != null) ? userPrincipalNameToUserCache.get(username) : null;
        if (user != null) {
            /**
             * TODO: bizybot If authorizing realms configured, resolve user from those
             * realms and then return.
             */
            listener.onResponse(AuthenticationResult.success(user));
        } else {
            /**
             * TODO: bizybot If authorizing realms configured, resolve user from those
             * realms, cache it and then return.
             */
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(username, null, Collections.emptySet(), null, this.config);
            userRoleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
                final User computedUser = new User(username, roles.toArray(new String[roles.size()]), null, null, null, true);
                if (userPrincipalNameToUserCache != null) {
                    userPrincipalNameToUserCache.put(username, computedUser);
                }
                listener.onResponse(AuthenticationResult.success(computedUser));
            }, listener::onFailure));
        }
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        listener.onResponse(null);
    }
}