/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativePrivilegeStoreTests extends ESTestCase {

    private NativePrivilegeStore store;
    private List<ActionRequest> requests;
    private AtomicReference<ActionListener> listener;
    private Client client;

    @Before
    public void setup() {
        requests = new ArrayList<>();
        listener = new AtomicReference<>();
        client = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                NativePrivilegeStoreTests.this.requests.add(request);
                NativePrivilegeStoreTests.this.listener.set(listener);
            }
        };
        final SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isAvailable()).thenReturn(true);
        Mockito.doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments().length, equalTo(2));
            assertThat(invocationOnMock.getArguments()[1], instanceOf(Runnable.class));
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        Mockito.doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments().length, equalTo(2));
            assertThat(invocationOnMock.getArguments()[1], instanceOf(Runnable.class));
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        store = new NativePrivilegeStore(Settings.EMPTY, client, securityIndex);
    }

    @After
    public void cleanup() {
        client.close();
    }

    public void testGetSinglePrivilegeByName() throws Exception {
        final ApplicationPrivilegeDescriptor sourcePrivilege = new ApplicationPrivilegeDescriptor("myapp", "admin",
            newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()
        );

        final PlainActionFuture<ApplicationPrivilegeDescriptor> future = new PlainActionFuture<>();
        store.getPrivilege("myapp", "admin", future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(GetRequest.class));
        GetRequest request = (GetRequest) requests.get(0);
        assertThat(request.index(), equalTo(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));
        assertThat(request.id(), equalTo("application-privilege_myapp:admin"));

        final String docSource = Strings.toString(sourcePrivilege);
        listener.get().onResponse(new GetResponse(
            new GetResult(request.index(), request.id(), 0, 1, 1L, true,
                new BytesArray(docSource), emptyMap(), emptyMap())
        ));
        final ApplicationPrivilegeDescriptor getPrivilege = future.get(1, TimeUnit.SECONDS);
        assertThat(getPrivilege, equalTo(sourcePrivilege));
    }

    public void testGetMissingPrivilege() throws Exception {
        final PlainActionFuture<ApplicationPrivilegeDescriptor> future = new PlainActionFuture<>();
        store.getPrivilege("myapp", "admin", future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(GetRequest.class));
        GetRequest request = (GetRequest) requests.get(0);
        assertThat(request.index(), equalTo(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));
        assertThat(request.id(), equalTo("application-privilege_myapp:admin"));

        listener.get().onResponse(new GetResponse(
            new GetResult(request.index(), request.id(), UNASSIGNED_SEQ_NO, 0, -1,
                false, null, emptyMap(), emptyMap())
        ));
        final ApplicationPrivilegeDescriptor getPrivilege = future.get(1, TimeUnit.SECONDS);
        assertThat(getPrivilege, Matchers.nullValue());
    }

    public void testGetPrivilegesByApplicationName() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"terms\":{\"application\":[\"myapp\",\"yourapp\"]"));
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get().onResponse(new SearchResponse(new SearchResponseSections(
            new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
            null, null, false, false, null, 1),
        "_scrollId1", 1, 1, 0, 1, null, null));

        assertResult(sourcePrivileges, future);
    }

    public void testGetPrivilegesByWildcardApplicationName() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp-*", "yourapp"), null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"bool\":{\"should\":[{\"terms\":{\"application\":[\"yourapp\"]"));
        assertThat(query, containsString("{\"prefix\":{\"application\":{\"value\":\"myapp-\""));
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));

        final SearchHit[] hits = new SearchHit[0];
        listener.get().onResponse(new SearchResponse(new SearchResponseSections(
            new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
            null, null, false, false, null, 1),
        "_scrollId1", 1, 1, 0, 1, null, null));
    }

    public void testGetPrivilegesByStarApplicationName() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("*", "anything"), null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"exists\":{\"field\":\"application\""));
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));

        final SearchHit[] hits = new SearchHit[0];
        listener.get().onResponse(new SearchResponse(new SearchResponseSections(
            new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
            null, null, false, false, null, 1),
        "_scrollId1", 1, 1, 0, 1, null, null));
    }

    public void testGetAllPrivileges() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("app1", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app2", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app3", "all", newHashSet("*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(null, null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));
        assertThat(query, not(containsString("{\"terms\"")));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get().onResponse(new SearchResponse(new SearchResponseSections(
            new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
            null, null, false, false, null, 1),
            "_scrollId1", 1, 1, 0, 1, null, null));

        assertResult(sourcePrivileges, future);
    }

    public void testPutPrivileges() throws Exception {
        final List<ApplicationPrivilegeDescriptor> putPrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("app1", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app1", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app2", "all", newHashSet("*"), emptyMap())
        );

        final PlainActionFuture<Map<String, List<String>>> future = new PlainActionFuture<>();
        store.putPrivileges(putPrivileges, WriteRequest.RefreshPolicy.IMMEDIATE, future);
        assertThat(requests, iterableWithSize(putPrivileges.size()));
        assertThat(requests, everyItem(instanceOf(IndexRequest.class)));

        final List<IndexRequest> indexRequests = new ArrayList<>(requests.size());
        requests.stream().map(IndexRequest.class::cast).forEach(indexRequests::add);
        requests.clear();

        final ActionListener indexListener = listener.get();
        final String uuid = UUIDs.randomBase64UUID(random());
        for (int i = 0; i < putPrivileges.size(); i++) {
            ApplicationPrivilegeDescriptor privilege = putPrivileges.get(i);
            IndexRequest request = indexRequests.get(i);
            assertThat(request.indices(), arrayContaining(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));
            assertThat(request.id(), equalTo(
                "application-privilege_" + privilege.getApplication() + ":" + privilege.getName()
            ));
            final XContentBuilder builder = privilege.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), true);
            assertThat(request.source(), equalTo(BytesReference.bytes(builder)));
            final boolean created = privilege.getName().equals("user") == false;
            indexListener.onResponse(new IndexResponse(
                new ShardId(RestrictedIndicesNames.SECURITY_MAIN_ALIAS, uuid, i),
                request.id(), 1, 1, 1, created
            ));
        }

        assertBusy(() -> assertFalse(requests.isEmpty()), 1, TimeUnit.SECONDS);

        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(ClearRolesCacheRequest.class));
        listener.get().onResponse(null);

        final Map<String, List<String>> map = future.actionGet();
        assertThat(map.entrySet(), iterableWithSize(2));
        assertThat(map.get("app1"), iterableWithSize(1));
        assertThat(map.get("app2"), iterableWithSize(1));
        assertThat(map.get("app1"), contains("admin"));
        assertThat(map.get("app2"), contains("all"));
    }

    public void testDeletePrivileges() throws Exception {
        final List<String> privilegeNames = Arrays.asList("p1", "p2", "p3");

        final PlainActionFuture<Map<String, List<String>>> future = new PlainActionFuture<>();
        store.deletePrivileges("app1", privilegeNames, WriteRequest.RefreshPolicy.IMMEDIATE, future);
        assertThat(requests, iterableWithSize(privilegeNames.size()));
        assertThat(requests, everyItem(instanceOf(DeleteRequest.class)));

        final List<DeleteRequest> deletes = new ArrayList<>(requests.size());
        requests.stream().map(DeleteRequest.class::cast).forEach(deletes::add);
        requests.clear();

        final ActionListener deleteListener = listener.get();
        final String uuid = UUIDs.randomBase64UUID(random());
        for (int i = 0; i < privilegeNames.size(); i++) {
            String name = privilegeNames.get(i);
            DeleteRequest request = deletes.get(i);
            assertThat(request.indices(), arrayContaining(RestrictedIndicesNames.SECURITY_MAIN_ALIAS));
            assertThat(request.id(), equalTo("application-privilege_app1:" + name));
            final boolean found = name.equals("p2") == false;
            deleteListener.onResponse(new DeleteResponse(
                new ShardId(RestrictedIndicesNames.SECURITY_MAIN_ALIAS, uuid, i),
                request.id(), 1, 1, 1, found
            ));
        }

        assertBusy(() -> assertFalse(requests.isEmpty()), 1, TimeUnit.SECONDS);

        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(ClearRolesCacheRequest.class));
        listener.get().onResponse(null);

        final Map<String, List<String>> map = future.actionGet();
        assertThat(map.entrySet(), iterableWithSize(1));
        assertThat(map.get("app1"), iterableWithSize(2));
        assertThat(map.get("app1"), containsInAnyOrder("p1", "p3"));
    }

    private SearchHit[] buildHits(List<ApplicationPrivilegeDescriptor> sourcePrivileges) {
        final SearchHit[] hits = new SearchHit[sourcePrivileges.size()];
        for (int i = 0; i < hits.length; i++) {
            final ApplicationPrivilegeDescriptor p = sourcePrivileges.get(i);
            hits[i] = new SearchHit(i, "application-privilege_" + p.getApplication() + ":" + p.getName(), null, null);
            hits[i].sourceRef(new BytesArray(Strings.toString(p)));
        }
        return hits;
    }

    private void assertResult(List<ApplicationPrivilegeDescriptor> sourcePrivileges,
                              PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future) throws Exception {
        final Collection<ApplicationPrivilegeDescriptor> getPrivileges = future.get(1, TimeUnit.SECONDS);
        assertThat(getPrivileges, iterableWithSize(sourcePrivileges.size()));
        assertThat(new HashSet<>(getPrivileges), equalTo(new HashSet<>(sourcePrivileges)));
    }
}
