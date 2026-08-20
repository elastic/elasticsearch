/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.service.BuiltInServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_NAME_FIELD;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_SOURCE_FIELD;
import static org.elasticsearch.xpack.security.authc.service.ElasticServiceAccounts.ACCOUNTS;

public class ServiceAccountService {

    private static final Logger logger = LogManager.getLogger(ServiceAccountService.class);
    private static final int MIN_TOKEN_SECRET_LENGTH = 10;

    static final String USER_MANAGED_ACCOUNTS_UNAVAILABLE_MESSAGE =
        "user-managed service accounts are not available in this cluster configuration";

    private final Client client;
    @Nullable
    private final IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private final ServiceAccountTokenStore readOnlyServiceAccountTokenStore;
    @Nullable
    private final UserManagedServiceAccountStore userManagedServiceAccountStore;

    public ServiceAccountService(Client client, ServiceAccountTokenStore readOnlyServiceAccountTokenStore) {
        this(client, readOnlyServiceAccountTokenStore, null, null);
    }

    public ServiceAccountService(
        Client client,
        ServiceAccountTokenStore readOnlyServiceAccountTokenStore,
        @Nullable IndexServiceAccountTokenStore indexServiceAccountTokenStore,
        @Nullable UserManagedServiceAccountStore userManagedServiceAccountStore
    ) {
        if (userManagedServiceAccountStore != null && indexServiceAccountTokenStore == null) {
            throw new IllegalArgumentException(
                "cannot support user-managed service accounts without an index-backed service account token store"
            );
        }
        this.client = client;
        this.readOnlyServiceAccountTokenStore = readOnlyServiceAccountTokenStore;
        this.indexServiceAccountTokenStore = indexServiceAccountTokenStore;
        this.userManagedServiceAccountStore = userManagedServiceAccountStore;
    }

    public static boolean isBuiltInServiceAccountPrincipal(String principal) {
        return ACCOUNTS.containsKey(principal);
    }

    public static Collection<String> getBuiltInServiceAccountPrincipals() {
        return ACCOUNTS.keySet();
    }

    public static Map<String, BuiltInServiceAccount> getBuiltInServiceAccounts() {
        return Map.copyOf(ACCOUNTS);
    }

    /**
     * Retained under its original name for callers outside this repository. In-repo code uses
     * {@link #getBuiltInServiceAccounts()}, which is the canonical accessor and states which accounts are returned.
     */
    public static Map<String, ServiceAccount> getServiceAccounts() {
        return Map.copyOf(ACCOUNTS);
    }

    /**
     * Parses a token object from the content of a {@link ServiceAccountToken#asBearerString()} bearer string}.
     * This bearer string would typically be extracted from an HTTP authorization header.
     *
     * <p>
     * <strong>This method does not validate the credential, it simply parses it.</strong>
     * There is no guarantee that the {@link ServiceAccountToken#getSecret() secret} is valid,
     * or even that the {@link ServiceAccountToken#getAccountId() account} exists.
     * </p>
     * @param bearerString A raw token string (if this is from an HTTP header, then the <code>"Bearer "</code> prefix must be removed before
     *              calling this method.
     * @return An unvalidated token object.
     */
    public static ServiceAccountToken tryParseToken(SecureString bearerString) {
        try {
            if (bearerString == null) {
                return null;
            }
            return ServiceAccountToken.fromBearerString(bearerString);
        } catch (Exception e) {
            logger.trace("Cannot parse possible service account token", e);
            return null;
        }
    }

    public void authenticateToken(ServiceAccountToken serviceAccountToken, String nodeName, ActionListener<Authentication> listener) {
        logger.trace("attempt to authenticate service account token [{}]", serviceAccountToken.getQualifiedName());

        if (serviceAccountToken.getSecret().length() < MIN_TOKEN_SECRET_LENGTH) {
            logger.debug(
                "failing authentication for service account token [{}],"
                    + " the provided credential has length [{}]"
                    + " but a token's secret value must be at least [{}] characters",
                serviceAccountToken.getQualifiedName(),
                serviceAccountToken.getSecret().length(),
                MIN_TOKEN_SECRET_LENGTH
            );
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        if (isBuiltInNamespace(serviceAccountToken.getAccountId().namespace())) {
            authenticateBuiltInToken(serviceAccountToken, nodeName, listener);
        } else {
            authenticateUserManagedToken(serviceAccountToken, nodeName, listener);
        }
    }

    private void authenticateBuiltInToken(
        ServiceAccountToken serviceAccountToken,
        String nodeName,
        ActionListener<Authentication> listener
    ) {
        final ServiceAccount account = ACCOUNTS.get(serviceAccountToken.getAccountId().asPrincipal());
        if (account == null) {
            logger.debug("the [{}] service account does not exist", serviceAccountToken.getAccountId().asPrincipal());
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        readOnlyServiceAccountTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(storeAuthenticationResult -> {
            if (storeAuthenticationResult.isSuccess()) {
                listener.onResponse(
                    createAuthentication(account, serviceAccountToken, storeAuthenticationResult.getTokenSource(), nodeName)
                );
            } else {
                final ElasticsearchSecurityException e = createAuthenticationException(serviceAccountToken);
                logger.debug(e.getMessage());
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    private void authenticateUserManagedToken(
        ServiceAccountToken serviceAccountToken,
        String nodeName,
        ActionListener<Authentication> listener
    ) {
        final String principal = serviceAccountToken.getAccountId().asPrincipal();
        if (userManagedServiceAccountStore == null) {
            logger.debug("user-managed service account [{}] is not supported in this cluster configuration", principal);
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }
        userManagedServiceAccountStore.getByPrincipal(principal, ActionListener.wrap(account -> {
            if (account == null || account.enabled() == false) {
                logger.debug("the [{}] user-managed service account does not exist or is disabled", principal);
                listener.onFailure(createAuthenticationException(serviceAccountToken));
                return;
            }
            indexServiceAccountTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(storeAuthenticationResult -> {
                if (storeAuthenticationResult.isSuccess()) {
                    listener.onResponse(
                        createAuthentication(account, serviceAccountToken, storeAuthenticationResult.getTokenSource(), nodeName)
                    );
                } else {
                    final ElasticsearchSecurityException e = createAuthenticationException(serviceAccountToken);
                    logger.debug(e.getMessage());
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        }, listener::onFailure));
    }

    /**
     * Creates the account, or replaces an existing one of the same name wholesale.
     */
    public void putUserManagedAccount(
        ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<UserManagedServiceAccountStore.PutResult> listener
    ) {
        if (userManagedServiceAccountStore == null) {
            listener.onFailure(new IllegalStateException(USER_MANAGED_ACCOUNTS_UNAVAILABLE_MESSAGE));
            return;
        }
        userManagedServiceAccountStore.putAccount(accountId, roles, enabled, refreshPolicy, listener);
    }

    /**
     * A surviving token cannot authenticate once its account is gone, but recreating an account of the same name would
     * bring it back to life, which is what the token check refuses rather than any live credential. It is not atomic
     * with the delete: a token created in between survives, leaving the state {@code force} produces deliberately.
     */
    public void deleteUserManagedAccount(
        ServiceAccountId accountId,
        boolean force,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<Boolean> listener
    ) {
        if (userManagedServiceAccountStore == null) {
            listener.onFailure(new IllegalStateException(USER_MANAGED_ACCOUNTS_UNAVAILABLE_MESSAGE));
            return;
        }
        if (force) {
            userManagedServiceAccountStore.deleteAccount(accountId, refreshPolicy, listener);
            return;
        }
        indexServiceAccountTokenStore.hasTokensFor(accountId, listener.delegateFailureAndWrap((delegate, hasTokens) -> {
            if (hasTokens) {
                delegate.onFailure(
                    new IllegalArgumentException(
                        "cannot delete service account ["
                            + accountId
                            + "] because it has service tokens; delete the tokens first,"
                            + " or set force=true to delete the account and leave its tokens in place"
                    )
                );
            } else {
                userManagedServiceAccountStore.deleteAccount(accountId, refreshPolicy, delegate);
            }
        }));
    }

    public void createIndexToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't create token because index service account token store not configured");
        }
        if (isBuiltInNamespace(request.getNamespace())) {
            indexServiceAccountTokenStore.createBuiltInToken(authentication, request, listener);
            return;
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        resolveUserManagedAccount(
            accountId,
            listener.delegateFailureAndWrap(
                (delegate, account) -> indexServiceAccountTokenStore.createUserManagedToken(authentication, request, delegate)
            )
        );
    }

    /**
     * Deletes a service account token. Unlike creating one, this does not resolve the account first: a token can
     * outlive a force-deleted account, and those leftovers must remain removable — the credentials API lists them, so
     * refusing to delete them would leave an operator able to see a token they cannot clean up.
     */
    public void deleteIndexToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't delete token because index service account token store not configured");
        }
        if (isBuiltInNamespace(request.getNamespace())) {
            indexServiceAccountTokenStore.deleteBuiltInToken(request, listener);
        } else {
            indexServiceAccountTokenStore.deleteUserManagedToken(request, listener);
        }
    }

    public void findTokensFor(GetServiceAccountCredentialsRequest request, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't find tokens because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        findIndexTokens(accountId, listener);
    }

    // TODO: No production code usage
    public static void getRoleDescriptor(Authentication authentication, ActionListener<RoleDescriptor> listener) {
        assert authentication.isServiceAccount() : "authentication is not for service account: " + authentication;
        final String principal = authentication.getEffectiveSubject().getUser().principal();
        getRoleDescriptorForPrincipal(principal, listener);
    }

    public static void getRoleDescriptorForPrincipal(String principal, ActionListener<RoleDescriptor> listener) {
        final BuiltInServiceAccount account = ACCOUNTS.get(principal);
        if (account == null) {
            listener.onFailure(
                new ElasticsearchSecurityException("cannot load role for service account [" + principal + "] - no such service account")
            );
            return;
        }
        listener.onResponse(account.roleDescriptor());
    }

    private static boolean isBuiltInNamespace(String namespace) {
        return ElasticServiceAccounts.NAMESPACE.equals(namespace);
    }

    /**
     * Resolves a user-managed account, failing when none exists. Whether the account is enabled is deliberately not
     * consulted: that state governs authentication, and refusing to issue a credential for a suspended account would
     * only prevent an operator from preparing one for the account's return. A token issued while the account is
     * disabled cannot authenticate until it is enabled again.
     *
     * <p>
     * A node without an account store cannot hold a user-managed account, so it reports absence rather than an
     * unavailable feature. The caller named an account that does not exist, which is a fault in the request and the
     * same answer this API gave for every unknown account before the namespace fork existed.
     * </p>
     */
    private void resolveUserManagedAccount(ServiceAccountId accountId, ActionListener<UserManagedServiceAccount> listener) {
        if (userManagedServiceAccountStore == null) {
            listener.onFailure(accountDoesNotExist(accountId));
            return;
        }
        userManagedServiceAccountStore.getByPrincipal(accountId.asPrincipal(), listener.delegateFailureAndWrap((delegate, account) -> {
            if (account == null) {
                delegate.onFailure(accountDoesNotExist(accountId));
            } else {
                delegate.onResponse(account);
            }
        }));
    }

    private static IllegalArgumentException accountDoesNotExist(ServiceAccountId accountId) {
        return new IllegalArgumentException("service account [" + accountId + "] does not exist");
    }

    private static Authentication createAuthentication(
        ServiceAccount account,
        ServiceAccountToken token,
        TokenSource tokenSource,
        String nodeName
    ) {
        final User user = account.asUser();
        return Authentication.newServiceAccountAuthentication(
            user,
            nodeName,
            Map.of(TOKEN_NAME_FIELD, token.getTokenName(), TOKEN_SOURCE_FIELD, tokenSource.name().toLowerCase(Locale.ROOT))
        );
    }

    private static ElasticsearchSecurityException createAuthenticationException(ServiceAccountToken serviceAccountToken) {
        return new ElasticsearchSecurityException(
            "failed to authenticate service account [{}] with token name [{}]",
            RestStatus.UNAUTHORIZED,
            serviceAccountToken.getAccountId().asPrincipal(),
            serviceAccountToken.getTokenName()
        );
    }

    private void findIndexTokens(ServiceAccountId accountId, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        indexServiceAccountTokenStore.findTokensFor(accountId, ActionListener.wrap(indexTokenInfos -> {
            findFileTokens(indexTokenInfos, accountId, listener);
        }, listener::onFailure));
    }

    private void findFileTokens(
        Collection<TokenInfo> indexTokenInfos,
        ServiceAccountId accountId,
        ActionListener<GetServiceAccountCredentialsResponse> listener
    ) {
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            GetServiceAccountNodesCredentialsAction.INSTANCE,
            new GetServiceAccountCredentialsNodesRequest(accountId.namespace(), accountId.serviceName()),
            ActionListener.wrap(
                fileTokensResponse -> listener.onResponse(
                    new GetServiceAccountCredentialsResponse(accountId.asPrincipal(), indexTokenInfos, fileTokensResponse)
                ),
                listener::onFailure
            )
        );
    }
}
