/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.oidc;

import com.nimbusds.jwt.JWT;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectTestCase;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.security.authc.TokenServiceTests.mockGetTokenFromAccessTokenBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportOpenIdConnectLogoutActionTests extends OpenIdConnectTestCase {

    private OpenIdConnectRealm oidcRealm;
    private TokenService tokenService;
    private List<IndexRequest> indexRequests;
    private List<BulkRequest> bulkRequests;
    private Client client;
    private TransportOpenIdConnectLogoutAction action;

    @Before
    public void setup() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("oidc", REALM_NAME);
        final Settings settings = getBasicRealmSettings().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("path.home", createTempDir())
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Settings sslSettings = Settings.builder()
            .put("xpack.security.authc.realms.oidc.oidc-realm.ssl.verification_mode", "certificate")
            .put("path.home", createTempDir())
            .build();
        final ThreadContext threadContext = new ThreadContext(settings);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        AuthenticationTestHelper.builder()
            .user(new User("kibana"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false)
            .writeToContext(threadContext);
        indexRequests = new ArrayList<>();
        bulkRequests = new ArrayList<>();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0]).setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(nullable(String.class), nullable(String.class));
        doAnswer(invocationOnMock -> {
            IndexRequestBuilder builder = new IndexRequestBuilder(client, IndexAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0]);
            return builder;
        }).when(client).prepareIndex(nullable(String.class));
        doAnswer(invocationOnMock -> {
            UpdateRequestBuilder builder = new UpdateRequestBuilder(client, UpdateAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0]).setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareUpdate(nullable(String.class), anyString());
        doAnswer(invocationOnMock -> {
            BulkRequestBuilder builder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
            return builder;
        }).when(client).prepareBulk();
        doAnswer(invocationOnMock -> {
            IndexRequest indexRequest = (IndexRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[1];
            indexRequests.add(indexRequest);
            final IndexResponse response = new IndexResponse(indexRequest.shardId(), indexRequest.id(), 1, 1, 1, true);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).index(any(IndexRequest.class), anyActionListener());
        doAnswer(invocationOnMock -> {
            IndexRequest indexRequest = (IndexRequest) invocationOnMock.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            indexRequests.add(indexRequest);
            final IndexResponse response = new IndexResponse(new ShardId("test", "test", 0), indexRequest.id(), 1, 1, 1, true);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), anyActionListener());
        doAnswer(invocationOnMock -> {
            BulkRequest bulkRequest = (BulkRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocationOnMock.getArguments()[1];
            bulkRequests.add(bulkRequest);
            final BulkResponse response = new BulkResponse(new BulkItemResponse[0], 1);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).bulk(any(BulkRequest.class), anyActionListener());

        final SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        doAnswer(inv -> {
            ((Runnable) inv.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(inv -> {
            ((Runnable) inv.getArguments()[1]).run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);

        final ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

        final MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);

        tokenService = new TokenService(
            settings,
            Clock.systemUTC(),
            client,
            licenseState,
            new SecurityContext(settings, threadContext),
            securityIndex,
            securityIndex,
            clusterService
        );

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final Realms realms = mock(Realms.class);
        action = new TransportOpenIdConnectLogoutAction(transportService, mock(ActionFilters.class), realms, tokenService);

        final Environment env = TestEnvironment.newEnvironment(settings);

        final RealmConfig realmConfig = new RealmConfig(realmIdentifier, settings, env, threadContext);
        oidcRealm = new OpenIdConnectRealm(
            realmConfig,
            new SSLService(TestEnvironment.newEnvironment(sslSettings)),
            mock(UserRoleMapper.class),
            mock(ResourceWatcherService.class)
        );
        when(realms.realm(realmConfig.name())).thenReturn(oidcRealm);
    }

    public void testLogoutInvalidatesTokens() throws Exception {
        final String subject = randomAlphaOfLength(8);
        final JWT signedIdToken = generateIdToken(subject, randomAlphaOfLength(8), randomAlphaOfLength(8));
        final User user = new User("oidc-user", new String[] { "superuser" }, null, null, Map.of(), true);
        final Authentication.RealmRef realmRef = new Authentication.RealmRef(oidcRealm.name(), OpenIdConnectRealmSettings.TYPE, "node01");
        final Map<String, Object> tokenMetadata = new HashMap<>();
        tokenMetadata.put("id_token_hint", signedIdToken.serialize());
        tokenMetadata.put("oidc_realm", REALM_NAME);
        final Authentication authentication = Authentication.newRealmAuthentication(user, realmRef);

        final PlainActionFuture<TokenService.CreateTokenResult> future = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        tokenService.createOAuth2Tokens(newTokenBytes.v1(), newTokenBytes.v2(), authentication, authentication, tokenMetadata, future);
        final String accessToken = future.actionGet().getAccessToken();
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, tokenMetadata, false, null, client);

        final OpenIdConnectLogoutRequest request = new OpenIdConnectLogoutRequest();
        request.setToken(accessToken);

        final PlainActionFuture<OpenIdConnectLogoutResponse> listener = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, listener);
        final OpenIdConnectLogoutResponse response = listener.get();
        assertNotNull(response);
        assertThat(response.getEndSessionUrl(), notNullValue());
        // One index request to create the token
        assertThat(indexRequests, hasSize(1));
        final IndexRequest indexRequest = indexRequests.get(0);
        assertThat(indexRequest, notNullValue());
        assertThat(indexRequest.id(), startsWith("token"));
        // One bulk request (containing one update request) to invalidate the token
        assertThat(bulkRequests, hasSize(1));
        final BulkRequest bulkRequest = bulkRequests.get(0);
        assertThat(bulkRequest.requests(), hasSize(1));
        assertThat(bulkRequest.requests().get(0), instanceOf(UpdateRequest.class));
        assertThat(bulkRequest.requests().get(0).id(), startsWith("token_"));
        assertThat(bulkRequest.requests().get(0).toString(), containsString("\"access_token\":{\"invalidated\":true"));
    }

    @After
    public void cleanup() {
        oidcRealm.close();
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
