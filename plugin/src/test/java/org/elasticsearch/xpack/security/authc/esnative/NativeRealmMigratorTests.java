/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NativeRealmMigratorTests extends ESTestCase {

    private Map<String, Map<String, Object>> reservedUsers;
    private InternalClient internalClient;
    private Client mockClient;
    private NativeRealmMigrator migrator;
    private XPackLicenseState licenseState;

    @Before
    public void setupMocks() throws IOException {
        final boolean allowClearCache = randomBoolean();
        mockClient = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        internalClient = new InternalClient(Settings.EMPTY, threadPool, mockClient);
        doAnswer(invocationOnMock -> {
            SearchRequest request = (SearchRequest) invocationOnMock.getArguments()[1];
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            if (request.indices().length == 1 && request.indices()[0].equals(SecurityTemplateService.SECURITY_INDEX_NAME)) {
                SearchResponse response = new SearchResponse() {
                    @Override
                    public SearchHits getHits() {
                        List<SearchHit> hits = reservedUsers.entrySet().stream()
                                .map((info) -> {
                                    SearchHit hit = new SearchHit(randomInt(), info.getKey(), null, null, emptyMap());
                                    try {
                                        hit.sourceRef(JsonXContent.contentBuilder().map(info.getValue()).bytes());
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                    return hit;
                                }).collect(Collectors.toList());
                        return new SearchHits(hits.toArray(new SearchHit[0]), (long) reservedUsers.size(), 0.0f);
                    }
                };
                listener.onResponse(response);
            } else {
                listener.onResponse(null);
            }
            return Void.TYPE;
        }).when(mockClient).execute(eq(SearchAction.INSTANCE), any(SearchRequest.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            GetRequest request = (GetRequest) invocationOnMock.getArguments()[1];
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            if (request.indices().length == 1 && request.indices()[0].equals(SecurityTemplateService.SECURITY_INDEX_NAME)
                    && request.type().equals(NativeUsersStore.RESERVED_USER_DOC_TYPE)) {
                final boolean exists = reservedUsers.get(request.id()) != null;
                GetResult getResult = new GetResult(SecurityTemplateService.SECURITY_INDEX_NAME, NativeUsersStore.RESERVED_USER_DOC_TYPE,
                        request.id(), randomLong(), exists, JsonXContent.contentBuilder().map(reservedUsers.get(request.id())).bytes(),
                        emptyMap());
                listener.onResponse(new GetResponse(getResult));
            } else {
                listener.onResponse(null);
            }
            return Void.TYPE;
        }).when(mockClient).execute(eq(GetAction.INSTANCE), any(GetRequest.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(mockClient).execute(eq(ClearRealmCacheAction.INSTANCE), any(ClearRealmCacheRequest.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(mockClient).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));
        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return Void.TYPE;
        }).when(mockClient).execute(eq(UpdateAction.INSTANCE), any(UpdateRequest.class), any(ActionListener.class));

        final Settings settings = Settings.EMPTY;

        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(allowClearCache);

        migrator = new NativeRealmMigrator(settings, licenseState, internalClient);
    }

    public void testNoChangeOnFreshInstall() throws Exception {
        verifyUpgrade(null, false, false);
    }

    public void testNoChangeOnUpgradeAfterV5_3() throws Exception {
        verifyUpgrade(randomFrom(Version.V_6_0_0_alpha1_UNRELEASED), false, false);
    }

    public void testDisableLogstashAndConvertPasswordsOnUpgradeFromVersionPriorToV5_2() throws Exception {
        this.reservedUsers = Collections.singletonMap(
                KibanaUser.NAME,
                MapBuilder.<String, Object>newMapBuilder()
                        .put(User.Fields.PASSWORD.getPreferredName(), new String(Hasher.BCRYPT.hash(ReservedRealm.DEFAULT_PASSWORD_TEXT)))
                        .put(User.Fields.ENABLED.getPreferredName(), false)
                        .immutableMap()
        );
        verifyUpgrade(randomFrom(Version.V_5_1_1_UNRELEASED, Version.V_5_0_2, Version.V_5_0_0), true, true);
    }

    public void testConvertPasswordsOnUpgradeFromVersion5_2() throws Exception {
        this.reservedUsers = randomSubsetOf(randomIntBetween(0, 3), LogstashSystemUser.NAME, KibanaUser.NAME, ElasticUser.NAME)
                .stream().collect(Collectors.toMap(Function.identity(),
                        name -> MapBuilder.<String, Object>newMapBuilder()
                                .put(User.Fields.PASSWORD.getPreferredName(),
                                        new String(Hasher.BCRYPT.hash(ReservedRealm.DEFAULT_PASSWORD_TEXT)))
                                .put(User.Fields.ENABLED.getPreferredName(), randomBoolean())
                                .immutableMap()
                ));
        verifyUpgrade(Version.V_5_2_0_UNRELEASED, false, true);
    }

    private void verifyUpgrade(Version fromVersion, boolean disableLogstashUser, boolean convertDefaultPasswords) throws Exception {
        final PlainActionFuture<Boolean> future = doUpgrade(fromVersion);
        boolean expectedResult = false;
        if (disableLogstashUser) {
            final boolean clearCache = licenseState.isAuthAllowed();
            ArgumentCaptor<GetRequest> captor = ArgumentCaptor.forClass(GetRequest.class);
            verify(mockClient).execute(eq(GetAction.INSTANCE), captor.capture(), any(ActionListener.class));
            assertEquals(LogstashSystemUser.NAME, captor.getValue().id());
            ArgumentCaptor<IndexRequest> indexCaptor = ArgumentCaptor.forClass(IndexRequest.class);
            verify(mockClient).execute(eq(IndexAction.INSTANCE), indexCaptor.capture(), any(ActionListener.class));
            assertEquals(LogstashSystemUser.NAME, indexCaptor.getValue().id());
            assertEquals(false, indexCaptor.getValue().sourceAsMap().get(User.Fields.ENABLED.getPreferredName()));

            if (clearCache) {
                verify(mockClient).execute(eq(ClearRealmCacheAction.INSTANCE), any(ClearRealmCacheRequest.class),
                        any(ActionListener.class));
            }
            expectedResult = true;
        }
        if (convertDefaultPasswords) {
            verify(mockClient).execute(eq(SearchAction.INSTANCE), any(SearchRequest.class), any(ActionListener.class));
            ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
            verify(mockClient, times(this.reservedUsers.size()))
                    .execute(eq(UpdateAction.INSTANCE), captor.capture(), any(ActionListener.class));
            final List<UpdateRequest> requests = captor.getAllValues();
            this.reservedUsers.keySet().forEach(u -> {
                UpdateRequest request = requests.stream().filter(r -> r.id().equals(u)).findFirst().get();
                assertThat(request.validate(), nullValue(ActionRequestValidationException.class));
                assertThat(request.doc().sourceAsMap(), hasEntry(is(User.Fields.PASSWORD.getPreferredName()), is("")));
                assertThat(request.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
            });
            expectedResult = true;
        }
        verifyNoMoreInteractions(mockClient);
        assertThat(future.get(), is(expectedResult));
    }

    private PlainActionFuture<Boolean> doUpgrade(Version fromVersion) {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        migrator.performUpgrade(fromVersion, future);
        return future;
    }
}
