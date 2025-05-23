/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.graph.core.requests.BaseGraphRequestAdapter;
import com.microsoft.graph.core.tasks.PageIterator;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.GroupCollectionResponse;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.authentication.AzureIdentityAuthenticationProvider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MicrosoftGraphAuthzRealm extends Realm {

    private static final Logger logger = LogManager.getLogger(MicrosoftGraphAuthzRealm.class);

    private final RealmConfig config;
    private final UserRoleMapper roleMapper;
    private final SecureString clientSecret;

    public MicrosoftGraphAuthzRealm(UserRoleMapper roleMapper, RealmConfig config) {
        super(config);

        this.config = config;
        this.roleMapper = roleMapper;
        this.clientSecret = config.getSetting(MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET);

        require(MicrosoftGraphAuthzRealmSettings.CLIENT_ID);
        require(MicrosoftGraphAuthzRealmSettings.TENANT_ID);

        if (clientSecret.isEmpty()) {
            throw new SettingsException(
                "The configuration setting ["
                    + RealmSettings.getFullSettingKey(config, MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET)
                    + "] is required"
            );
        }

        // FIXME both of these lines are load bearing, because this project is cursed
        new com.google.gson.JsonParser();
        kotlin.jvm.internal.Intrinsics.checkParameterIsNotNull(clientSecret, "clientSecret");
        // TODO license check
    }

    private void require(Setting.AffixSetting<String> setting) {
        final var value = config.getSetting(setting);
        if (value.isEmpty()) {
            throw new SettingsException("The configuration setting [" + RealmSettings.getFullSettingKey(config, setting) + "] is required");
        }
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return false;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        listener.onResponse(AuthenticationResult.notHandled());
    }

    @Override
    public void lookupUser(String principal, ActionListener<User> listener) {
        try {
            // TODO probably want to do this once rather than every time
            final var client = buildClient();
            final var userProperties = sdkFetchUserProperties(client, principal);
            final var groups = sdkFetchGroupMembership(client, principal);

            // TODO confirm we don't need any other fields
            final var userData = new UserRoleMapper.UserData(principal, null, groups, Map.of(), config);

            roleMapper.resolveRoles(userData, listener.delegateFailureAndWrap((l, roles) -> {
                final var user = new User(
                    principal,
                    roles.toArray(Strings.EMPTY_ARRAY),
                    userProperties.v1(),
                    userProperties.v2(),
                    Map.of(),
                    true
                );
                logger.trace("Entra ID user {}", user);
                l.onResponse(user);
            }));
        } catch (Exception e) {
            // TODO logging etc
            listener.onFailure(e);
        }
    }

    private GraphServiceClient buildClient() {
        logger.trace("building client");
        final var credentialProvider = new ClientSecretCredentialBuilder().clientId(
            config.getSetting(MicrosoftGraphAuthzRealmSettings.CLIENT_ID)
        )
            .clientSecret(clientSecret.toString())
            .tenantId(config.getSetting(MicrosoftGraphAuthzRealmSettings.TENANT_ID))
            .authorityHost(config.getSetting(MicrosoftGraphAuthzRealmSettings.ACCESS_TOKEN_HOST))
            // TODO this is necessary for tests, but we probably want this enabled in prod
            .disableInstanceDiscovery()
            .build();

        return new GraphServiceClient(
            new BaseGraphRequestAdapter(
                new AzureIdentityAuthenticationProvider(credentialProvider, Strings.EMPTY_ARRAY, "https://graph.microsoft.com/.default"),
                config.getSetting(MicrosoftGraphAuthzRealmSettings.API_HOST)
            )
        );
    }

    private Tuple<String, String> sdkFetchUserProperties(GraphServiceClient client, String userId) {
        var response = client.users().byUserId(userId)
            .get(requestConfig -> requestConfig.queryParameters.select = new String[] { "displayName", "mail" });

        logger.trace("User [{}] has email [{}]", response.getDisplayName(), response.getMail());

        return Tuple.tuple(response.getDisplayName(), response.getMail());
    }

    private List<String> sdkFetchGroupMembership(GraphServiceClient client, String userId) throws ReflectiveOperationException {
        List<String> groups = new ArrayList<>();

        // TODO figure out exactly what we need to fetch here - we may need to fetch transitive groups as well, and may need to remove
        //  the `graph.group` cast (i.e. fetch "directory roles" and "administrative units" as well);
        //  see https://learn.microsoft.com/en-us/graph/api/user-list-transitivememberof
        var groupMembership = client.users().byUserId(userId).memberOf().graphGroup().get(requestConfig -> {
            requestConfig.queryParameters.select = new String[] { "id" };
            requestConfig.queryParameters.top = 999;
        });

        var pageIterator = new PageIterator.Builder<Group, GroupCollectionResponse>().client(client)
            .collectionPage(groupMembership)
            .collectionPageFactory(GroupCollectionResponse::createFromDiscriminatorValue)
            .requestConfigurator(requestInfo -> {
                requestInfo.addQueryParameter("%24select", new String[] { "id" });
                requestInfo.addQueryParameter("%24top", "999");
                return requestInfo;
            })
            .processPageItemCallback(group -> {
                groups.add(group.getId());
                return true;
            })
            .build();

        pageIterator.iterate();

        return groups;
    }
}
