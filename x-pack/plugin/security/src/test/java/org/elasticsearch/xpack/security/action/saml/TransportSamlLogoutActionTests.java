/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.saml.SamlNameId;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealmTests;
import org.elasticsearch.xpack.security.authc.saml.SamlTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.security.authc.TokenServiceTests.mockGetTokenFromId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSamlLogoutActionTests extends SamlTestCase {

    private static final String SP_URL = "https://sp.example.net/saml";
    private static final String REALM_NAME = "saml1";

    private SamlRealm samlRealm;
    private TokenService tokenService;
    private List<IndexRequest> indexRequests;
    private List<BulkRequest> bulkRequests;
    private TransportSamlLogoutAction action;
    private Client client;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        final RealmIdentifier realmIdentifier = new RealmIdentifier("saml", REALM_NAME);
        final Path metadata = PathUtils.get(SamlRealm.class.getResource("idp1.xml").toURI());
        final Settings settings = Settings.builder()
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("path.home", createTempDir())
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), metadata.toString())
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID), SamlRealmTests.TEST_IDP_ENTITY_ID)
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.SP_ENTITY_ID), SP_URL)
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.SP_ACS), SP_URL)
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute()), "uid")
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();

        final ThreadContext threadContext = new ThreadContext(settings);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        new Authentication(new User("kibana"), new Authentication.RealmRef("realm", "type", "node"), null).writeToContext(threadContext);

        indexRequests = new ArrayList<>();
        bulkRequests = new ArrayList<>();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0])
                    .setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString());
        doAnswer(invocationOnMock -> {
            IndexRequestBuilder builder = new IndexRequestBuilder(client, IndexAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0]);
            return builder;
        }).when(client).prepareIndex(anyString());
        doAnswer(invocationOnMock -> {
            UpdateRequestBuilder builder = new UpdateRequestBuilder(client, UpdateAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0])
                    .setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareUpdate(anyString(), anyString());
        doAnswer(invocationOnMock -> {
            BulkRequestBuilder builder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
            return builder;
        }).when(client).prepareBulk();
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[1];
            MultiGetResponse response = mock(MultiGetResponse.class);
            MultiGetItemResponse[] responses = new MultiGetItemResponse[2];
            when(response.getResponses()).thenReturn(responses);

            GetResponse oldGetResponse = mock(GetResponse.class);
            when(oldGetResponse.isExists()).thenReturn(false);
            responses[0] = new MultiGetItemResponse(oldGetResponse, null);

            GetResponse getResponse = mock(GetResponse.class);
            responses[1] = new MultiGetItemResponse(getResponse, null);
            when(getResponse.isExists()).thenReturn(false);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).multiGet(any(MultiGetRequest.class), any(ActionListener.class));
        doAnswer(invocationOnMock -> {
            IndexRequest indexRequest = (IndexRequest) invocationOnMock.getArguments()[0];
            ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[1];
            indexRequests.add(indexRequest);
            final IndexResponse response = new IndexResponse(
                new ShardId("test", "test", 0), indexRequest.id(), 1, 1, 1, true);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));
        doAnswer(invocationOnMock -> {
            IndexRequest indexRequest = (IndexRequest) invocationOnMock.getArguments()[1];
            ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            indexRequests.add(indexRequest);
            final IndexResponse response = new IndexResponse(
                new ShardId("test", "test", 0), indexRequest.id(), 1, 1, 1, true);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));
        doAnswer(invocationOnMock -> {
            BulkRequest bulkRequest = (BulkRequest) invocationOnMock.getArguments()[0];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocationOnMock.getArguments()[1];
            bulkRequests.add(bulkRequest);
            final BulkResponse response = new BulkResponse(new BulkItemResponse[0], 1);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).bulk(any(BulkRequest.class), any(ActionListener.class));

        final SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        doAnswer(inv -> {
            ((Runnable) inv.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer(inv -> {
            ((Runnable) inv.getArguments()[1]).run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);

        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE)).thenReturn(true);
        final ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        final SecurityContext securityContext = new SecurityContext(settings, threadContext);
        tokenService = new TokenService(settings, Clock.systemUTC(), client, licenseState, securityContext, securityIndex, securityIndex,
            clusterService);

        final TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final Realms realms = mock(Realms.class);
        action = new TransportSamlLogoutAction(transportService, mock(ActionFilters.class), realms, tokenService);

        final Environment env = TestEnvironment.newEnvironment(settings);

        final RealmConfig realmConfig = new RealmConfig(realmIdentifier, settings, env, threadContext);
        samlRealm = SamlRealm.create(realmConfig, mock(SSLService.class), mock(ResourceWatcherService.class), mock(UserRoleMapper.class));
        when(realms.realm(realmConfig.name())).thenReturn(samlRealm);
    }

    @After
    public void cleanup() {
        samlRealm.close();
    }

    public void testLogoutInvalidatesToken() throws Exception {
        final String session = randomAlphaOfLengthBetween(12, 18);
        final String nameId = randomAlphaOfLengthBetween(6, 16);
        final Map<String, Object> userMetadata = MapBuilder.<String, Object>newMapBuilder()
                .put(SamlRealm.USER_METADATA_NAMEID_FORMAT, NameID.TRANSIENT)
                .put(SamlRealm.USER_METADATA_NAMEID_VALUE, nameId)
                .map();
        final User user = new User("punisher", new String[]{"superuser"}, null, null, userMetadata, true);
        final Authentication.RealmRef realmRef = new Authentication.RealmRef(samlRealm.name(), SamlRealmSettings.TYPE, "node01");
        final Map<String, Object> tokenMetadata = samlRealm.createTokenMetadata(
            new SamlNameId(NameID.TRANSIENT, nameId, null, null, null), session);
        final Authentication authentication = new Authentication(user, realmRef, null, null, Authentication.AuthenticationType.REALM,
            tokenMetadata);


        final PlainActionFuture<TokenService.CreateTokenResult> future = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, tokenMetadata, future);
        final String accessToken = future.actionGet().getAccessToken();
        mockGetTokenFromId(tokenService, userTokenId, authentication, false, client);

        final SamlLogoutRequest request = new SamlLogoutRequest();
        request.setToken(accessToken);
        final PlainActionFuture<SamlLogoutResponse> listener = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, listener);
        final SamlLogoutResponse response = listener.get();
        assertThat(response, notNullValue());
        assertThat(response.getRequestId(), notNullValue());
        assertThat(response.getRedirectUrl(), notNullValue());

        final IndexRequest indexRequest1 = indexRequests.get(0);
        assertThat(indexRequest1, notNullValue());
        assertThat(indexRequest1.id(), startsWith("token"));

        assertThat(bulkRequests.size(), equalTo(1));

        final BulkRequest bulkRequest = bulkRequests.get(0);
        assertThat(bulkRequest.requests().size(), equalTo(1));
        assertThat(bulkRequest.requests().get(0), instanceOf(UpdateRequest.class));
        assertThat(bulkRequest.requests().get(0).id(), startsWith("token_"));
        assertThat(bulkRequest.requests().get(0).toString(), containsString("\"access_token\":{\"invalidated\":true"));
    }

}
