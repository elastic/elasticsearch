/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

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
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudIamRealm extends Realm implements CachingRealm {
    private final ThreadPool threadPool;
    private final UserRoleMapper roleMapper;
    private final IamClient iamClient;
    private final String signedHeader;
    private final boolean roleMappingEnabled;
    private final boolean allowAssumedRole;
    private final TimeValue allowedSkew;
    private final int signedHeaderMaxBytes;
    private final Cache<String, User> userCache;
    private final Cache<String, Boolean> negativeCache;
    private final Cache<String, Boolean> nonceCache;
    private final String[] staticRoles;

    public CloudIamRealm(RealmConfig config, ThreadPool threadPool, UserRoleMapper roleMapper, IamClient iamClient) {
        super(config);
        this.threadPool = threadPool;
        this.roleMapper = roleMapper;
        this.iamClient = iamClient;
        this.signedHeader = config.getSetting(CloudIamRealmSettings.SIGNED_HEADER, () -> "X-ES-IAM-Signed");
        this.roleMappingEnabled = config.getSetting(CloudIamRealmSettings.ROLE_MAPPING_ENABLED);
        this.allowAssumedRole = config.getSetting(CloudIamRealmSettings.ALLOW_ASSUMED_ROLE);
        this.allowedSkew = config.getSetting(CloudIamRealmSettings.ALLOWED_SKEW);
        this.signedHeaderMaxBytes = Math.toIntExact(config.getSetting(CloudIamRealmSettings.SIGNED_HEADER_MAX_BYTES).getBytes());
        this.userCache = buildUserCache(config);
        this.negativeCache = buildNegativeCache(config);
        this.nonceCache = buildNonceCache(config);
        List<String> roleList = config.getSetting(CloudIamRealmSettings.MOCK_ROLES);
        this.staticRoles = roleList.toArray(new String[0]);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof CloudIamToken;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        String signedRequest = context.getHeader(signedHeader);
        if (Strings.hasText(signedRequest) == false) {
            return null;
        }
        return CloudIamToken.fromHeaders(signedRequest, signedHeaderMaxBytes);
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        if (token instanceof CloudIamToken == false) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        CloudIamToken iamToken = (CloudIamToken) token;
        if (iamToken.isValid() == false) {
            listener.onResponse(AuthenticationResult.terminate("invalid cloud iam token: " + iamToken.validationError()));
            return;
        }
        if (isTimestampValid(iamToken.timestamp()) == false) {
            listener.onResponse(AuthenticationResult.terminate("invalid iam token timestamp"));
            return;
        }
        String nonce = iamToken.nonce();
        if (Strings.hasText(nonce) && nonceCache != null) {
            if (nonceCache.get(nonce) != null) {
                listener.onResponse(AuthenticationResult.terminate("replayed iam token"));
                return;
            }
            nonceCache.put(nonce, Boolean.TRUE);
        }
        String cacheKey = cacheKey(iamToken);
        if (negativeCache != null && negativeCache.get(cacheKey) != null) {
            listener.onResponse(AuthenticationResult.terminate("cached iam failure"));
            return;
        }
        if (userCache != null) {
            User cachedUser = userCache.get(cacheKey);
            if (cachedUser != null) {
                listener.onResponse(AuthenticationResult.success(cachedUser));
                return;
            }
        }
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> iamClient.verify(iamToken, ActionListener.wrap(principal -> {
            if (allowAssumedRole == false && principal.principalType() == IamPrincipal.PrincipalType.ASSUMED_ROLE) {
                listener.onResponse(AuthenticationResult.terminate("assumed role is not allowed"));
                return;
            }
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("cloud_arn", principal.arn());
            metadata.put("cloud_account", principal.accountId());
            metadata.put("cloud_principal_type", principal.principalType().name().toLowerCase(java.util.Locale.ROOT));
            if (principal.userId() != null) {
                metadata.put("cloud_user_id", principal.userId());
            }
            resolveRoles(principal, metadata, cacheKey, listener);
        }, e -> {
            cacheFailure(cacheKey);
            listener.onResponse(AuthenticationResult.terminate("IAM verify failed", e));
        })));
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }

    @Override
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {
        if (roleMappingEnabled && roleMapper != null) {
            roleMapper.clearRealmCacheOnChange(this);
        }
    }

    @Override
    public void expire(String username) {
        if (userCache != null) {
            userCache.invalidate(username);
        }
        if (negativeCache != null) {
            negativeCache.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        if (userCache != null) {
            userCache.invalidateAll();
        }
        if (negativeCache != null) {
            negativeCache.invalidateAll();
        }
        if (nonceCache != null) {
            nonceCache.invalidateAll();
        }
    }

    private void resolveRoles(
        IamPrincipal principal,
        Map<String, Object> metadata,
        String cacheKey,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        if (roleMappingEnabled && roleMapper != null) {
            UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal.arn(), null, List.of(), metadata, config);
            roleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
                if (rolesSet.isEmpty()) {
                    listener.onResponse(AuthenticationResult.terminate("no roles mapped"));
                    return;
                }
                User user = new User(principal.arn(), rolesSet.toArray(Strings.EMPTY_ARRAY), null, null, metadata, true);
                cacheUser(cacheKey, user);
                listener.onResponse(AuthenticationResult.success(user));
            }, e -> {
                cacheFailure(cacheKey);
                listener.onResponse(AuthenticationResult.terminate("role mapping failed", e));
            }));
            return;
        }
        User user = new User(principal.arn(), staticRoles, null, null, metadata, true);
        cacheUser(cacheKey, user);
        listener.onResponse(AuthenticationResult.success(user));
    }

    private boolean isTimestampValid(Instant timestamp) {
        if (timestamp == null) {
            return false;
        }
        Duration skew = Duration.between(timestamp, Instant.now()).abs();
        return skew.toMillis() <= allowedSkew.getMillis();
    }

    private String cacheKey(CloudIamToken token) {
        StringBuilder key = new StringBuilder(token.accessKeyId());
        if (Strings.hasText(token.sessionToken())) {
            key.append(':').append(token.sessionToken());
        }
        return key.toString();
    }

    private void cacheUser(String key, User user) {
        if (userCache != null) {
            userCache.put(key, user);
        }
    }

    private void cacheFailure(String key) {
        if (negativeCache != null) {
            negativeCache.put(key, Boolean.TRUE);
        }
    }

    private Cache<String, User> buildUserCache(RealmConfig config) {
        TimeValue ttl = config.getSetting(CloudIamRealmSettings.CACHE_TTL);
        if (ttl.getNanos() <= 0) {
            return null;
        }
        int maxEntries = config.getSetting(CloudIamRealmSettings.CACHE_MAX_ENTRIES);
        return CacheBuilder.<String, User>builder().setExpireAfterWrite(ttl).setMaximumWeight(maxEntries).build();
    }

    private Cache<String, Boolean> buildNegativeCache(RealmConfig config) {
        TimeValue ttl = config.getSetting(CloudIamRealmSettings.NEGATIVE_CACHE_TTL);
        if (ttl.getNanos() <= 0) {
            return null;
        }
        int maxEntries = config.getSetting(CloudIamRealmSettings.CACHE_MAX_ENTRIES);
        return CacheBuilder.<String, Boolean>builder().setExpireAfterWrite(ttl).setMaximumWeight(maxEntries).build();
    }

    private Cache<String, Boolean> buildNonceCache(RealmConfig config) {
        TimeValue ttl = config.getSetting(CloudIamRealmSettings.NONCE_TTL);
        if (ttl.getNanos() <= 0) {
            return null;
        }
        int maxEntries = config.getSetting(CloudIamRealmSettings.NONCE_MAX_ENTRIES);
        return CacheBuilder.<String, Boolean>builder().setExpireAfterWrite(ttl).setMaximumWeight(maxEntries).build();
    }
}
