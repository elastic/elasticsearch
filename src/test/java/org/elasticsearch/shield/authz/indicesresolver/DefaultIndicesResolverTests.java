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
import org.elasticsearch.cluster.metadata.AliasAction;
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

import static org.hamcrest.Matchers.*;
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
        when(authzService.authorizedIndicesAndAliases(userNoIndices, IndicesAliasesAction.NAME)).thenReturn(ImmutableList.<String>of());
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
    public void testResolveIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo", "foofoo");
        request.addAlias("alias2", "foo", "foobar");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"alias1", "alias2", "foo", "foofoo", "foobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("foo", "foobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("alias2"));
    }

    @Test
    public void testResolveIndicesAliasesRequestExistingAlias() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo", "foofoo");
        request.addAlias("foofoobar", "foo", "foobar");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned, foofoobar is an existing alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias1", "foofoobar", "foo", "foofoo", "foobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("foo", "foobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("foofoobar"));
    }

    @Test
    public void testResolveIndicesAliasesRequestMissingIndex() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo", "foofoo");
        request.addAlias("alias2", "missing");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned, missing is not an existing index/alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias1", "alias2", "foo", "foofoo", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("missing"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("alias2"));
    }

    @Test
    public void testResolveWildcardsIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo*");
        request.addAlias("alias2", "bar*");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "alias2", "foofoo", "foofoobar", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foofoobar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("alias2"));
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveWildcardsIndicesAliasesRequestNoMatchingIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo*");
        request.addAlias("alias2", "bar*");
        request.addAlias("alias3", "non_matching_*");
        //if a single operation contains wildcards and ends up being resolved to no indices, it makes the whole request fail
        defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
    }

    @Test
    public void testResolveAllIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "_all");
        request.addAlias("alias2", "_all");
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo", "alias1", "alias2"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        //_all gets replaced with all indices that user is authorized for, on each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining(replacedIndices));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining(replacedIndices));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("alias2"));
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveAllIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "_all");
        //current user is not authorized for any index, _all resolves to no indices, the request fails
        defaultIndicesResolver.resolve(userNoIndices, IndicesAliasesAction.NAME, request, metaData);
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveWildcardsIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo*");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        defaultIndicesResolver.resolve(userNoIndices, IndicesAliasesAction.NAME, request, metaData);
    }

    @Test
    public void testResolveIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("foo", "foofoobar"));
        request.addAliasAction(AliasAction.newRemoveAliasAction("foofoo", "barbaz"));
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"foo", "foofoobar", "foofoo", "barbaz"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("foofoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("barbaz"));
    }

    @Test
    public void testResolveIndicesAliasesRequestDeleteActionsMissingIndex() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("foo", "foofoobar"));
        request.addAliasAction(AliasAction.newRemoveAliasAction("missing_index", "missing_alias"));
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned, doesn't matter is some of them don't exist
        String[] expectedIndices = new String[]{"foo", "foofoobar", "missing_index", "missing_alias"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("missing_index"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("missing_alias"));
    }

    @Test
    public void testResolveWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("foo*", "foofoobar"));
        request.addAliasAction(AliasAction.newRemoveAliasAction("bar*", "barbaz"));
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        String[] expectedIndices = new String[]{"foofoobar", "foofoo", "bar", "barbaz"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced within each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foofoobar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("barbaz"));
    }

    @Test
    public void testResolveAliasesWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("*", "foo*"));
        request.addAliasAction(AliasAction.newRemoveAliasAction("*bar", "foo*"));
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("bar", "foofoobar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("bar", "foofoobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("foofoobar"));
    }

    @Test(expected = IndexMissingException.class)
    public void testResolveAliasesWildcardsIndicesAliasesRequestDeleteActionsNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("foo*", "foo*"));
        //no authorized aliases match bar*, hence this action fails and makes the whole request fail
        request.addAliasAction(AliasAction.newRemoveAliasAction("*bar", "bar*"));
        defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
    }

    @Test
    public void testResolveWildcardsIndicesAliasesRequestAddAndDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("foo*", "foofoobar"));
        request.addAliasAction(AliasAction.newAddAliasAction("bar*", "foofoobar"));
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        String[] expectedIndices = new String[]{"foofoobar", "foofoo", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //every single action has its indices replaced with matching (authorized) ones
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("foofoobar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContaining("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("foofoobar"));
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
        assertThat(request.indices(), arrayContaining(expectedIndices));
    }

    @Test
    public void testResolveWriteAction() {
        DeleteByQueryRequest request = new DeleteByQueryRequest("*");
        Set<String> indices = defaultIndicesResolver.resolve(user, DeleteByQueryAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContaining(expectedIndices));
    }

    private static IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }
}
