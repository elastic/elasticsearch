/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;
import org.ietf.jgss.GSSException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public static final String KRB_METADATA_REALM_NAME_KEY = "kerberos_realm";
    public static final String KRB_METADATA_UPN_KEY = "kerberos_user_principal_name";

    private final Cache<String, User> userPrincipalNameToUserCache;
    private final UserRoleMapper userRoleMapper;
    private final KerberosTicketValidator kerberosTicketValidator;
    private final ThreadPool threadPool;
    private final Path keytabPath;
    private final boolean enableKerberosDebug;
    private final boolean removeRealmName;
    private DelegatedAuthorizationSupport delegatedRealms;

    public KerberosRealm(final RealmConfig config, final UserRoleMapper userRoleMapper, final ThreadPool threadPool) {
        this(config, userRoleMapper, new KerberosTicketValidator(), threadPool, null);
    }

    // pkg scoped for testing
    KerberosRealm(
        final RealmConfig config,
        final UserRoleMapper userRoleMapper,
        final KerberosTicketValidator kerberosTicketValidator,
        final ThreadPool threadPool,
        final Cache<String, User> userPrincipalNameToUserCache
    ) {
        super(config);
        this.userRoleMapper = userRoleMapper;
        this.userRoleMapper.clearRealmCacheOnChange(this);
        final TimeValue ttl = config.getSetting(KerberosRealmSettings.CACHE_TTL_SETTING);
        if (ttl.getNanos() > 0) {
            this.userPrincipalNameToUserCache = (userPrincipalNameToUserCache == null)
                ? CacheBuilder.<String, User>builder()
                    .setExpireAfterWrite(config.getSetting(KerberosRealmSettings.CACHE_TTL_SETTING))
                    .setMaximumWeight(config.getSetting(KerberosRealmSettings.CACHE_MAX_USERS_SETTING))
                    .build()
                : userPrincipalNameToUserCache;
        } else {
            this.userPrincipalNameToUserCache = null;
        }
        this.kerberosTicketValidator = kerberosTicketValidator;
        this.threadPool = threadPool;
        this.keytabPath = config.env().configDir().resolve(config.getSetting(KerberosRealmSettings.HTTP_SERVICE_KEYTAB_PATH));

        validateKeytab(this.keytabPath);

        this.enableKerberosDebug = config.getSetting(KerberosRealmSettings.SETTING_KRB_DEBUG_ENABLE);
        this.removeRealmName = config.getSetting(KerberosRealmSettings.SETTING_REMOVE_REALM_NAME);
        this.delegatedRealms = null;
    }

    private static void validateKeytab(Path keytabPath) {
        boolean fileExists = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> Files.exists(keytabPath));
        if (fileExists == false) {
            throw new IllegalArgumentException("configured service key tab file [" + keytabPath + "] does not exist");
        }
        boolean pathIsDir = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> Files.isDirectory(keytabPath));
        if (pathIsDir) {
            throw new IllegalArgumentException("configured service key tab file [" + keytabPath + "] is a directory");
        }
        boolean isReadable = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> Files.isReadable(keytabPath));
        if (isReadable == false) {
            throw new IllegalArgumentException("configured service key tab file [" + keytabPath + "] must have read permission");
        }
    }

    @Override
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {
        if (delegatedRealms != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        delegatedRealms = new DelegatedAuthorizationSupport(realms, config, licenseState);
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
    public void authenticate(final AuthenticationToken token, final ActionListener<AuthenticationResult<User>> listener) {
        assert delegatedRealms != null : "Realm has not been initialized correctly";
        assert token instanceof KerberosAuthenticationToken;
        final KerberosAuthenticationToken kerbAuthnToken = (KerberosAuthenticationToken) token;
        kerberosTicketValidator.validateTicket(
            (byte[]) kerbAuthnToken.credentials(),
            keytabPath,
            enableKerberosDebug,
            ActionListener.wrap(userPrincipalNameOutToken -> {
                if (userPrincipalNameOutToken.v1() != null) {
                    resolveUser(userPrincipalNameOutToken.v1(), userPrincipalNameOutToken.v2(), listener);
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
            }, e -> handleException(e, listener))
        );
    }

    private static String[] splitUserPrincipalName(final String userPrincipalName) {
        return userPrincipalName.split("@");
    }

    private void handleException(Exception e, final ActionListener<AuthenticationResult<User>> listener) {
        if (e instanceof LoginException) {
            logger.debug("failed to authenticate user, service login failure", e);
            listener.onResponse(
                AuthenticationResult.terminate(
                    "failed to authenticate user, service login failure",
                    unauthorized(e.getLocalizedMessage(), e)
                )
            );
        } else if (e instanceof GSSException) {
            logger.debug("failed to authenticate user, gss context negotiation failure", e);
            listener.onResponse(
                AuthenticationResult.terminate(
                    "failed to authenticate user, gss context negotiation failure",
                    unauthorized(e.getLocalizedMessage(), e)
                )
            );
        } else {
            logger.debug("failed to authenticate user", e);
            listener.onFailure(e);
        }
    }

    private void resolveUser(
        final String userPrincipalName,
        final String outToken,
        final ActionListener<AuthenticationResult<User>> listener
    ) {
        // if outToken is present then it needs to be communicated with peer, add it to
        // response header in thread context.
        if (Strings.hasText(outToken)) {
            threadPool.getThreadContext().addResponseHeader(WWW_AUTHENTICATE, NEGOTIATE_AUTH_HEADER_PREFIX + outToken);
        }

        final String[] userAndRealmName = splitUserPrincipalName(userPrincipalName);
        /*
         * Usually principal names are in the form 'user/instance@REALM'. If
         * KerberosRealmSettings#SETTING_REMOVE_REALM_NAME is true then remove
         * '@REALM' part from the user principal name to get username.
         */
        final String username = (this.removeRealmName) ? userAndRealmName[0] : userPrincipalName;

        if (delegatedRealms.hasDelegation()) {
            delegatedRealms.resolve(username, listener);
        } else {
            final User user = (userPrincipalNameToUserCache != null) ? userPrincipalNameToUserCache.get(username) : null;
            if (user != null) {
                listener.onResponse(AuthenticationResult.success(user));
            } else if (userAndRealmName.length > 1) {
                final String realmName = userAndRealmName[1];
                buildUser(username, Map.of(KRB_METADATA_REALM_NAME_KEY, realmName, KRB_METADATA_UPN_KEY, userPrincipalName), listener);
            } else {
                buildUser(username, Map.of(KRB_METADATA_UPN_KEY, userPrincipalName), listener);
            }
        }
    }

    private void buildUser(
        final String username,
        final Map<String, Object> metadata,
        final ActionListener<AuthenticationResult<User>> listener
    ) {
        final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(username, null, Set.of(), metadata, this.config);
        userRoleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
            final User computedUser = new User(username, roles.toArray(new String[roles.size()]), null, null, userData.getMetadata(), true);
            if (userPrincipalNameToUserCache != null) {
                userPrincipalNameToUserCache.put(username, computedUser);
            }
            listener.onResponse(AuthenticationResult.success(computedUser));
        }, listener::onFailure));
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        listener.onResponse(null);
    }
}
