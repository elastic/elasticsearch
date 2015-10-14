/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultIndicesResolverTests extends ESTestCase {
    private User user;
    private User userNoIndices;
    private MetaData metaData;
    private DefaultIndicesAndAliasesResolver defaultIndicesResolver;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 2))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
                .build();

        MetaData.Builder mdBuilder = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("closed").state(IndexMetaData.State.CLOSE).putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE).settings(settings))
                .put(indexBuilder("foobar-closed").state(IndexMetaData.State.CLOSE).settings(settings))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")).settings(settings))
                .put(indexBuilder("bar").settings(settings))
                .put(indexBuilder("bar-closed").state(IndexMetaData.State.CLOSE).settings(settings))
                .put(indexBuilder("bar2").settings(settings));
        metaData = mdBuilder.build();

        AuthorizationService authzService = mock(AuthorizationService.class);
        user = new User.Simple("user", new String[] { "role" });

        String[] authorizedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed"};
        when(authzService.authorizedIndicesAndAliases(user, SearchAction.NAME)).thenReturn(Collections.unmodifiableList(Arrays.asList(authorizedIndices)));
        when(authzService.authorizedIndicesAndAliases(user, MultiSearchAction.NAME)).thenReturn(Collections.unmodifiableList(Arrays.asList(authorizedIndices)));
        when(authzService.authorizedIndicesAndAliases(user, MultiGetAction.NAME)).thenReturn(Collections.unmodifiableList(Arrays.asList(authorizedIndices)));
        when(authzService.authorizedIndicesAndAliases(user, IndicesAliasesAction.NAME)).thenReturn(Collections.unmodifiableList(Arrays.asList(authorizedIndices)));
        when(authzService.authorizedIndicesAndAliases(user, GetAliasesAction.NAME)).thenReturn(Collections.unmodifiableList(Arrays.asList(authorizedIndices)));
        when(authzService.authorizedIndicesAndAliases(user, DeleteIndexAction.NAME)).thenReturn(Collections.unmodifiableList(Arrays.asList(authorizedIndices)));
        userNoIndices = new User.Simple("test", new String[] { "test" });
        when(authzService.authorizedIndicesAndAliases(userNoIndices, IndicesAliasesAction.NAME)).thenReturn(Collections.<String>emptyList());
        when(authzService.authorizedIndicesAndAliases(userNoIndices, GetAliasesAction.NAME)).thenReturn(Collections.<String>emptyList());
        when(authzService.authorizedIndicesAndAliases(userNoIndices, SearchAction.NAME)).thenReturn(Collections.<String>emptyList());
        when(authzService.authorizedIndicesAndAliases(userNoIndices, MultiSearchAction.NAME)).thenReturn(Collections.<String>emptyList());

        defaultIndicesResolver = new DefaultIndicesAndAliasesResolver(authzService);
    }

    public void testResolveEmptyIndicesExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveEmptyIndicesExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveAllExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveAllExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveWildcardsExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveWildcardsExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveWildcardsMinusExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("-foofoo*");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveWildcardsMinusExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("-foofoo*");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveWildcardsPlusAndMinusExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("-foofoo*", "+barbaz", "+foob*");
        request.indicesOptions(randomFrom(IndicesOptions.strictExpandOpen(), IndicesOptions.lenientExpandOpen()));
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveWildcardsPlusAndMinusExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("-foofoo*", "+barbaz");
        request.indicesOptions(IndicesOptions.strictExpand());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContaining(replacedIndices));
    }

    public void testResolveNonMatchingIndices() {
        SearchRequest request = new SearchRequest("missing*");
        try {
            defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveNoAuthorizedIndices() {
        SearchRequest request = new SearchRequest();
        try {
            defaultIndicesResolver.resolve(userNoIndices, SearchAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveMissingIndex() {
        SearchRequest request = new SearchRequest("bar*", "missing");
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNonMatchingIndicesAndExplicit() {
        SearchRequest request = new SearchRequest("missing*", "bar");
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNoExpand() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        Set<String> indices = defaultIndicesResolver.resolve(user, SearchAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"missing*"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

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

    public void testResolveWildcardsIndicesAliasesRequestNoMatchingIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo*");
        request.addAlias("alias2", "bar*");
        request.addAlias("alias3", "non_matching_*");
        //if a single operation contains wildcards and ends up being resolved to no indices, it makes the whole request fail
        try {
            defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

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

    public void testResolveAllIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "_all");
        //current user is not authorized for any index, _all resolves to no indices, the request fails
        try {
            defaultIndicesResolver.resolve(userNoIndices, IndicesAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveWildcardsIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("alias1", "foo*");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        try {
            defaultIndicesResolver.resolve(userNoIndices, IndicesAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

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

    public void testResolveAllAliasesWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("*", "_all"));
        request.addAliasAction(new IndicesAliasesRequest.AliasActions(AliasAction.Type.REMOVE, "_all", new String[]{"_all", "explicit"}));
        Set<String> indices = defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo", "explicit"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("bar", "foofoobar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContaining("foofoobar"));
        assertThat(request.getAliasActions().get(0).indices(), arrayContaining("bar", "foofoobar", "foofoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("foofoobar", "explicit"));
    }

    public void testResolveAliasesWildcardsIndicesAliasesRequestDeleteActionsNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasAction.newRemoveAliasAction("foo*", "foo*"));
        //no authorized aliases match bar*, hence this action fails and makes the whole request fail
        request.addAliasAction(AliasAction.newRemoveAliasAction("*bar", "bar*"));
        try {
            defaultIndicesResolver.resolve(user, IndicesAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

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

    public void testResolveGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest("alias1").indices("foo", "foofoo");
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"alias1", "foo", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContaining("foo", "foofoo"));
        assertThat(request.aliases(), arrayContaining("alias1"));
    }

    public void testResolveGetAliasesRequestMissingIndex() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("missing");
        request.aliases("alias2");
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all indices and aliases gets returned, missing is not an existing index/alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias2", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContaining("missing"));
        assertThat(request.aliases(), arrayContaining("alias2"));
    }

    public void testResolveWildcardsGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("alias1");
        request.indices("foo*");
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContaining("foofoobar", "foofoo"));
        assertThat(request.aliases(), arrayContaining("alias1"));
    }

    public void testResolveWildcardsGetAliasesRequestNoMatchingIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("alias3");
        request.indices("non_matching_*");
        //indices get resolved to no indices, request gets rejected
        try {
            defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveAllGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        //even if not set, empty means _all
        if (randomBoolean()) {
            request.indices("_all");
        }
        request.aliases("alias1");
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo", "alias1"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContaining(replacedIndices));
        assertThat(request.aliases(), arrayContaining("alias1"));
    }

    public void testResolveAllGetAliasesRequestExpandWildcardsClosed() {
        GetAliasesRequest request = new GetAliasesRequest();
        //set indices options to have wildcards resolved to open and closed indices (default is open only)
        request.indicesOptions(IndicesOptions.fromOptions(true, false, true, true));
        //even if not set, empty means _all
        if (randomBoolean()) {
            request.indices("_all");
        }
        request.aliases("alias1");
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed", "alias1"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContaining(replacedIndices));
        assertThat(request.aliases(), arrayContaining("alias1"));
    }

    public void testResolveAllGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("alias1");
        request.indices("_all");
        //current user is not authorized for any index, _all resolves to no indices, the request fails
        try {
            defaultIndicesResolver.resolve(userNoIndices, GetAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveWildcardsGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("alias1");
        request.indices("foo*");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        try {
            defaultIndicesResolver.resolve(userNoIndices, GetAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveAllAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        if (randomBoolean()) {
            request.aliases("_all");
        }
        if (randomBoolean()) {
            request.indices("_all");
        }
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContaining(expectedIndices));
        assertThat(request.aliases(), arrayContaining("foofoobar"));
    }

    public void testResolveAllAndExplicitAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "explicit"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo", "explicit"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContaining("bar", "foofoobar", "foofoo"));
        assertThat(request.aliases(), arrayContaining("foofoobar", "explicit"));
    }

    public void testResolveAllAndWildcardsAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "foo*", "non_matching_*"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContaining(expectedIndices));
        assertThat(request.aliases(), arrayContaining("foofoobar", "foofoobar"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("*bar");
        request.aliases("foo*");
        Set<String> indices = defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.indices(), arrayContaining("bar", "foofoobar"));
        assertThat(request.aliases(), arrayContaining("foofoobar"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        //no authorized aliases match bar*, hence the request fails
        request.aliases("bar*");
        request.indices("*bar");
        try {
            defaultIndicesResolver.resolve(user, GetAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveAliasesAllGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        if (randomBoolean()) {
            request.aliases("_all");
        }
        request.indices("non_existing");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        try {
            defaultIndicesResolver.resolve(userNoIndices, GetAliasesAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    //msearch is a CompositeIndicesRequest whose items (SearchRequests) implement IndicesRequest.Replaceable, wildcards will get replaced
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

    public void testResolveMultiSearchWildcardsNoMatchingIndices() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("missing*"));
        request.add(Requests.searchRequest("foobar"));
        try {
            defaultIndicesResolver.resolve(user, MultiSearchAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testResolveMultiSearchWildcardsNoAuthorizedIndices() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(Requests.searchRequest("foofoo*"));
        request.add(Requests.searchRequest("foobar"));
        try {
            defaultIndicesResolver.resolve(userNoIndices, MultiSearchAction.NAME, request, metaData);
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    //mget is a CompositeIndicesRequest whose items don't support expanding wildcards
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

    public void testResolveAdminAction() {
        DeleteIndexRequest request = new DeleteIndexRequest("*");
        Set<String> indices = defaultIndicesResolver.resolve(user, DeleteIndexAction.NAME, request, metaData);
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContaining(expectedIndices));
    }

    // TODO with the removal of DeleteByQuery is there another way to test resolving a write action?

    private static IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(Settings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }
}
