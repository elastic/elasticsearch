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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;
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
    /**
     * Returned when the {@link ManagedServiceAccountStore} is not wired up: either an extension has
     * replaced the token store, or the cluster supports multiple projects (see the store's Javadoc).
     */
    static final String MANAGED_ACCOUNTS_UNAVAILABLE_MESSAGE = "managed service accounts are not available in this cluster configuration";

    private final Client client;
    private final IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private final ServiceAccountTokenStore readOnlyServiceAccountTokenStore;
    @Nullable
    private final ManagedServiceAccountStore managedServiceAccountStore;

    public ServiceAccountService(
        Client client,
        ServiceAccountTokenStore readOnlyServiceAccountTokenStore,
        @Nullable IndexServiceAccountTokenStore indexServiceAccountTokenStore,
        @Nullable ManagedServiceAccountStore managedServiceAccountStore
    ) {
        this.client = client;
        this.readOnlyServiceAccountTokenStore = readOnlyServiceAccountTokenStore;
        this.indexServiceAccountTokenStore = indexServiceAccountTokenStore;
        this.managedServiceAccountStore = managedServiceAccountStore;
    }

    public static boolean isBuiltInServiceAccountPrincipal(String principal) {
        return ACCOUNTS.containsKey(principal);
    }

    public static boolean isServiceAccountPrincipal(String principal) {
        return ACCOUNTS.containsKey(principal);
    }

    public static Collection<String> getBuiltInServiceAccountPrincipals() {
        return ACCOUNTS.keySet();
    }

    public static Collection<String> getServiceAccountPrincipals() {
        return getBuiltInServiceAccountPrincipals();
    }

    public static Map<String, ServiceAccount> getBuiltInServiceAccounts() {
        return Map.copyOf(ACCOUNTS);
    }

    /**
     * @deprecated Retained for out-of-repo callers that predate the built-in/managed split.
     *             Use {@link #getBuiltInServiceAccounts()} instead.
     */
    @Deprecated
    public static Map<String, ServiceAccount> getServiceAccounts() {
        return getBuiltInServiceAccounts();
    }

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

        final ServiceAccountId accountId = serviceAccountToken.getAccountId();
        final String principal = accountId.asPrincipal();

        if (ElasticServiceAccounts.isBuiltInNamespace(accountId.namespace())) {
            authenticateBuiltInToken(serviceAccountToken, nodeName, listener);
            return;
        }

        if (managedServiceAccountStore == null || indexServiceAccountTokenStore == null) {
            logger.debug("managed service account [{}] is not supported in this configuration", principal);
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        if (ManagedServiceAccountIdValidator.validatePrincipal(principal) != null) {
            logger.debug("service account principal [{}] is not a valid managed service account", principal);
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        managedServiceAccountStore.getByPrincipal(principal, ActionListener.wrap(managedAccount -> {
            if (managedAccount == null || managedAccount.enabled() == false) {
                logger.debug("managed service account [{}] does not exist or is disabled", principal);
                listener.onFailure(createAuthenticationException(serviceAccountToken));
                return;
            }
            indexServiceAccountTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(storeAuthenticationResult -> {
                if (storeAuthenticationResult.isSuccess()) {
                    listener.onResponse(
                        createAuthentication(managedAccount, serviceAccountToken, storeAuthenticationResult.getTokenSource(), nodeName)
                    );
                } else {
                    logger.debug(
                        "failed to authenticate managed service account token [{}] for account [{}]",
                        serviceAccountToken.getQualifiedName(),
                        principal
                    );
                    listener.onFailure(createAuthenticationException(serviceAccountToken));
                }
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private void authenticateBuiltInToken(
        ServiceAccountToken serviceAccountToken,
        String nodeName,
        ActionListener<Authentication> listener
    ) {
        if (ElasticServiceAccounts.isBuiltInNamespace(serviceAccountToken.getAccountId().namespace()) == false) {
            logger.debug(
                "only [{}] service accounts are supported, but received [{}]",
                ElasticServiceAccounts.NAMESPACE,
                serviceAccountToken.getAccountId().asPrincipal()
            );
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

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

    public void putManagedAccount(PutManagedServiceAccountRequest request, ActionListener<PutManagedServiceAccountResponse> listener) {
        if (managedServiceAccountStore == null) {
            listener.onFailure(new IllegalArgumentException(MANAGED_ACCOUNTS_UNAVAILABLE_MESSAGE));
            return;
        }
        managedServiceAccountStore.putAccount(
            request.getAccountId(),
            request.getRoles(),
            request.isEnabled(),
            request.getRefreshPolicy(),
            ActionListener.wrap(
                result -> listener.onResponse(
                    new PutManagedServiceAccountResponse(
                        result.type() == ManagedServiceAccountStore.PutResult.Type.CREATED
                            ? PutManagedServiceAccountResponse.Result.CREATED
                            : PutManagedServiceAccountResponse.Result.UPDATED
                    )
                ),
                listener::onFailure
            )
        );
    }

    public void deleteManagedAccount(
        DeleteManagedServiceAccountRequest request,
        ActionListener<DeleteManagedServiceAccountResponse> listener
    ) {
        if (managedServiceAccountStore == null) {
            listener.onFailure(new IllegalArgumentException(MANAGED_ACCOUNTS_UNAVAILABLE_MESSAGE));
            return;
        }
        final ServiceAccountId accountId = request.getAccountId();
        if (request.isForce() || indexServiceAccountTokenStore == null) {
            doDeleteManagedAccount(request, listener);
            return;
        }
        // Refuse to delete an account that still has service tokens, so that a routine delete cannot
        // strand live credentials that would be re-enabled by recreating the same account name. This
        // is a bounded existence check that does not enumerate tokens; the credentials GET API lists
        // them. The token check and the delete are not atomic; a token created concurrently may
        // survive, which fails in the same direction as force=true.
        indexServiceAccountTokenStore.hasTokensFor(accountId, ActionListener.wrap(hasTokens -> {
            if (hasTokens) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "cannot delete service account ["
                            + accountId
                            + "] because it has service tokens; delete the tokens first,"
                            + " or set force=true to delete the account and leave its tokens in place"
                    )
                );
            } else {
                doDeleteManagedAccount(request, listener);
            }
        }, listener::onFailure));
    }

    private void doDeleteManagedAccount(
        DeleteManagedServiceAccountRequest request,
        ActionListener<DeleteManagedServiceAccountResponse> listener
    ) {
        managedServiceAccountStore.deleteAccount(
            request.getAccountId(),
            request.getRefreshPolicy(),
            ActionListener.wrap(deleted -> listener.onResponse(new DeleteManagedServiceAccountResponse(deleted)), listener::onFailure)
        );
    }

    public void createIndexToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't create token because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        if (ElasticServiceAccounts.isBuiltInPrincipal(accountId.asPrincipal())) {
            indexServiceAccountTokenStore.createBuiltInToken(authentication, request, listener);
            return;
        }
        if (managedServiceAccountStore == null) {
            listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
            return;
        }
        managedServiceAccountStore.getByPrincipal(accountId.asPrincipal(), ActionListener.wrap(managedAccount -> {
            if (managedAccount == null || managedAccount.enabled() == false) {
                listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
                return;
            }
            indexServiceAccountTokenStore.createManagedToken(authentication, request, listener);
        }, listener::onFailure));
    }

    public void deleteIndexToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't delete token because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        if (ElasticServiceAccounts.isBuiltInPrincipal(accountId.asPrincipal())) {
            indexServiceAccountTokenStore.deleteBuiltInToken(request, listener);
            return;
        }
        ensureManagedAccountExists(
            accountId,
            ActionListener.wrap(ignore -> indexServiceAccountTokenStore.deleteManagedToken(request, listener), listener::onFailure)
        );
    }

    public void findTokensFor(GetServiceAccountCredentialsRequest request, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't find tokens because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        findIndexTokens(accountId, listener);
    }

    public void getManagedAccountInfos(
        @Nullable String namespace,
        @Nullable String serviceName,
        ActionListener<List<ServiceAccountInfo>> listener
    ) {
        if (managedServiceAccountStore == null) {
            listener.onResponse(List.of());
            return;
        }
        managedServiceAccountStore.listAccounts(
            namespace,
            serviceName,
            ActionListener.wrap(
                accounts -> listener.onResponse(
                    accounts.stream()
                        .map(account -> ServiceAccountInfo.managed(account.id().asPrincipal(), account.roles(), account.enabled()))
                        .sorted(java.util.Comparator.comparing(ServiceAccountInfo::getPrincipal))
                        .toList()
                ),
                listener::onFailure
            )
        );
    }

    public static void getRoleDescriptor(Authentication authentication, ActionListener<RoleDescriptor> listener) {
        assert authentication.isServiceAccount() : "authentication is not for service account: " + authentication;
        if (authentication.isManagedServiceAccount()) {
            listener.onFailure(
                new ElasticsearchSecurityException(
                    "managed service accounts resolve privileges through named roles, not inline descriptors"
                )
            );
            return;
        }
        final String principal = authentication.getEffectiveSubject().getUser().principal();
        getRoleDescriptorForPrincipal(principal, listener);
    }

    public static void getRoleDescriptorForPrincipal(String principal, ActionListener<RoleDescriptor> listener) {
        final ServiceAccount account = ACCOUNTS.get(principal);
        if (account == null) {
            listener.onFailure(
                new ElasticsearchSecurityException("cannot load role for service account [" + principal + "] - no such service account")
            );
            return;
        }
        listener.onResponse(account.roleDescriptor());
    }

    private void ensureManagedAccountExists(ServiceAccountId accountId, ActionListener<Void> listener) {
        if (managedServiceAccountStore == null) {
            listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
            return;
        }
        managedServiceAccountStore.getByPrincipal(accountId.asPrincipal(), ActionListener.wrap(managedAccount -> {
            if (managedAccount == null) {
                listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
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
