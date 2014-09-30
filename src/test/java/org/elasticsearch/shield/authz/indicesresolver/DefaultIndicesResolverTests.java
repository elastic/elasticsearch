/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.arrayContaining;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultIndicesResolverTests extends ElasticsearchTestCase {

    private User user;
    private User userNoIndices;
    private MetaData metaData;
    private DefaultIndicesResolver defaultIndicesResolver;

    @Before
    public void setup() {
        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foofoobar")))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foobar-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")))
                .put(indexBuilder("bar"))
                .put(indexBuilder("bar-closed").state(IndexMetaData.State.CLOSE))
                .put(indexBuilder("bar2"));
        metaData = mdBuilder.build();

        AuthorizationService authzService = mock(AuthorizationService.class);
        user = new User.Simple("user", "role");

        String[] authorizedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed"};
        when(authzService.authorizedIndicesAndAliases(user, SearchAction.NAME)).thenReturn(ImmutableList.copyOf(authorizedIndices));
        when(authzService.authorizedIndicesAndAliases(user, MultiSearchAction.NAME)).thenReturn(ImmutableList.copyOf(authorizedIndices));
        when(authzService.authorizedIndicesAndAliases(user, MultiGetAction.NAME)).thenReturn(ImmutableList.copyOf(authorizedIndices));
        when(authzService.authorizedIndicesAndAliases(user, IndicesAliasesAction.NAME)).thenReturn(ImmutableList.copyOf(authorizedIndices));
        when(authzService.authorizedIndicesAndAliases(user, DeleteIndexAction.NAME)).thenReturn(ImmutableList.copyOf(authorizedIndices));
        when(authzService.authorizedIndicesAndAliases(user, DeleteByQueryAction.NAME)).thenReturn(ImmutableList.copyOf(authorizedIndices));
        userNoIndices = new User.Simple("test", "test");
        when(authzService.authorizedIndicesAndAliases(userNoIndices, SearchAction.NAME)).thenReturn(ImmutableList.<String>of());
        when(authzService.authorizedIndicesAndAliases(userNoIndices, MultiSearchAction.NAME)).thenReturn(ImmutableList.<String>of());

        defaultIndicesResolver = new DefaultIndicesResolver(authzService);
    }

    @Test
    public void testResolveEmptyIndicesExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveEmptyIndicesExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveAllExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveAllExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveWildcardsExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveWildcardsExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveWildcardsMinusExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("-foofoo*");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveWildcardsMinusExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("-foofoo*");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveWildcardsPlusAndMinusExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("-foofoo*", "+barbaz", "+foob*");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test
    public void testResolveWildcardsPlusAndMinusExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("-foofoo*", "+barbaz");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveNonMatchingIndices() {
        SearchRequest request = new SearchRequest("missing*");
        defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveNoAuthorizedIndices() {
        SearchRequest request = new SearchRequest();
        defaultIndicesResolver.resolve(userNoIndices, SearchAction.NAME, request, metaData);
    }

    @Test
    public void testResolveMissingIndex() {
        SearchRequest request = new SearchRequest("bar*", "missing");
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    @Test
    public void testResolveNonMatchingIndicesAndExplicit() {
        SearchRequest request = new SearchRequest("missing*", "bar");
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    @Test
    public void testResolveNoExpand() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"missing*"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    @Test
    public void testResolveAllIndicesRequestNonReplaceable() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias", "_all");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //we do expand _all (to concrete indices only!) but we don't replace them with authorized indices
        String[] expectedIndices = new String[]{"foofoo", "foo", "foobar", "bar", "bar2"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(new String[]{"_all"}));
    }

    @Test
    public void testResolveWildcardsIndicesRequestNonReplaceable() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias", "*");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //we do explode wildcards but we don't replace them with authorized indices
        String[] expectedIndices = new String[]{"foofoobar", "foobar", "barbaz", "foo", "foofoo", "bar", "bar2"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(new String[]{"*"}));
    }

    //msearch is a CompositeIndicesRequest whose items (SearchRequests) implement IndicesRequest.Replaceable, wildcards will get replaced
    @Test
    public void testResolveMultiSearchNoWildcards() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("foo", "bar"));
        request.add(Requests.searchRequest("bar2"));
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"foo", "bar", "bar2"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"foo", "bar"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"bar2"}));
    }

    @Test
    public void testResolveMultiSearchNoWildcardsMissingIndex() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("foo", "bar"));
        request.add(Requests.searchRequest("bar2"));
        request.add(Requests.searchRequest("missing"));
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"foo", "bar", "bar2", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"foo", "bar"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"bar2"}));
        assertThat(request.subRequests().get(2).indices(), equalTo(new String[]{"missing"}));
    }

    @Test
    public void testResolveMultiSearchWildcardsExpandOpen() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("bar*")).indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        request.add(Requests.searchRequest("foobar"));
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "foobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"bar"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"foobar"}));
    }

    @Test
    public void testResolveMultiSearchWildcardsExpandOpenAndClose() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("bar*").indicesOptions(IndicesOptions.strictExpand()));
        request.add(Requests.searchRequest("foobar"));
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"bar", "bar-closed"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"foobar"}));
    }

    @Test
    public void testResolveMultiSearchWildcardsMissingIndex() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("bar*"));
        request.add(Requests.searchRequest("missing"));
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"bar"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"missing"}));
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveMultiSearchWildcardsNoMatchingIndices() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("missing*"));
        request.add(Requests.searchRequest("foobar"));
        defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveMultiSearchWildcardsNoAuthorizedIndices() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("foofoo*"));
        request.add(Requests.searchRequest("foobar"));
        defaultIndicesResolver.resolve(userNoIndices, MultiSearchAction.NAME, request, metaData);
    }

    //mget is a CompositeIndicesRequest whose items don't support expanding wildcards
    @Test
    public void testResolveMultiGet() {
        MultiGetRequest request = new MultiGetRequest();
        request.add("foo", "type", "id");
        request.add("bar", "type", "id");
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiGetAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"foo", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"foo"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"bar"}));
    }

    @Test
    public void testResolveMultiGetMissingIndex() {
        MultiGetRequest request = new MultiGetRequest();
        request.add("foo", "type", "id");
        request.add("missing", "type", "id");
        Set<String> indices = defaultIndicesResolver.resolve(user, MultiGetAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"foo", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.subRequests().get(0).indices(), equalTo(new String[]{"foo"}));
        assertThat(request.subRequests().get(1).indices(), equalTo(new String[]{"missing"}));
    }

    @Test
    public void testResolveAdminAction() {
        DeleteIndexRequest request = new DeleteIndexRequest("*");
        Set<String> indices = defaultIndicesResolver.resolve(user, DeleteIndexAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(request.indices(), arrayContaining(expectedIndices));
    }

    @Test
    public void testResolveWriteAction() {
        DeleteByQueryRequest request = new DeleteByQueryRequest("*");
        Set<String> indices = defaultIndicesResolver.resolve(user, DeleteByQueryAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(request.indices(), arrayContaining(expectedIndices));
    }

    private static IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }
}
