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
import com.microsoft.graph.models.odataerrors.ODataError;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.authentication.AzureIdentityAuthenticationProvider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;
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
import java.util.stream.Collectors;

public class MicrosoftGraphAuthzRealm extends Realm {

    private static final Logger logger = LogManager.getLogger(MicrosoftGraphAuthzRealm.class);

    private static final boolean DISABLE_INSTANCE_DISCOVERY = System.getProperty(
        "tests.azure.credentials.disable_instance_discovery",
        "false"
    ).equals("true");

    static final LicensedFeature.Momentary MICROSOFT_GRAPH_FEATURE = LicensedFeature.momentary(
        "security-realms",
        "microsoft_graph",
        License.OperationMode.PLATINUM
    );

    private final RealmConfig config;
    private final UserRoleMapper roleMapper;
    private final GraphServiceClient client;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;

    public MicrosoftGraphAuthzRealm(UserRoleMapper roleMapper, RealmConfig config, ThreadPool threadPool) {
        super(config);

        this.config = config;
        this.roleMapper = roleMapper;
        var clientSecret = config.getSetting(MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET);

        require(MicrosoftGraphAuthzRealmSettings.CLIENT_ID);
        require(MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET);
        require(MicrosoftGraphAuthzRealmSettings.TENANT_ID);

        this.client = buildClient(clientSecret);
        this.licenseState = XPackPlugin.getSharedLicenseState();
        this.threadPool = threadPool;
    }

    // for testing
    MicrosoftGraphAuthzRealm(
        UserRoleMapper roleMapper,
        RealmConfig config,
        GraphServiceClient client,
        XPackLicenseState licenseState,
        ThreadPool threadPool
    ) {
        super(config);
        this.config = config;
        this.roleMapper = roleMapper;
        this.client = client;
        this.licenseState = licenseState;
        this.threadPool = threadPool;
    }

    private <T> void require(Setting.AffixSetting<T> setting) {
        final var value = config.getSetting(setting);
        if (value.toString().isEmpty()) {
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
        if (MICROSOFT_GRAPH_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(MICROSOFT_GRAPH_FEATURE.getName()));
            return;
        }

        threadPool.generic().execute(() -> {
            try {
                final var userProperties = fetchUserProperties(client, principal);
                final var groups = fetchGroupMembership(client, principal);

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
                    logger.trace("Authorized user from Microsoft Graph {}", user);
                    l.onResponse(user);
                }));
            } catch (ReflectiveOperationException | ODataError e) {
                logger.error("failed to authenticate with realm", e);
                listener.onFailure(e);
            }
        });
    }

    private GraphServiceClient buildClient(SecureString clientSecret) {
        final var credentialProviderBuilder = new ClientSecretCredentialBuilder().clientId(
            config.getSetting(MicrosoftGraphAuthzRealmSettings.CLIENT_ID)
        )
            .clientSecret(clientSecret.toString())
            .tenantId(config.getSetting(MicrosoftGraphAuthzRealmSettings.TENANT_ID))
            .authorityHost(config.getSetting(MicrosoftGraphAuthzRealmSettings.ACCESS_TOKEN_HOST));

        if (DISABLE_INSTANCE_DISCOVERY) {
            credentialProviderBuilder.disableInstanceDiscovery();
        }
        final var credentialProvider = credentialProviderBuilder.build();

        return new GraphServiceClient(
            new BaseGraphRequestAdapter(
                new AzureIdentityAuthenticationProvider(credentialProvider, Strings.EMPTY_ARRAY, "https://graph.microsoft.com/.default"),
                config.getSetting(MicrosoftGraphAuthzRealmSettings.API_HOST)
            )
        );
    }

    private Tuple<String, String> fetchUserProperties(GraphServiceClient client, String userId) {
        var response = client.users()
            .byUserId(userId)
            .get(requestConfig -> requestConfig.queryParameters.select = new String[] { "displayName", "mail" });

        logger.trace("Fetched user with name [{}] and email [{}] from Microsoft Graph", response.getDisplayName(), response.getMail());

        return Tuple.tuple(response.getDisplayName(), response.getMail());
    }

    private List<String> fetchGroupMembership(GraphServiceClient client, String userId) throws ReflectiveOperationException {
        List<String> groups = new ArrayList<>();

        var groupMembership = client.users().byUserId(userId).transitiveMemberOf().graphGroup().get(requestConfig -> {
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

        if (logger.isTraceEnabled()) {
            logger.trace("Fetched [{}] groups from Microsoft Graph: [{}]", groups.size(), String.join(", ", groups));
        }

        return groups;
    }
}
