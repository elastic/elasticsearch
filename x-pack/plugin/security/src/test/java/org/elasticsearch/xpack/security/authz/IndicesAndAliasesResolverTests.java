/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData.State;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesAndAliasesResolverTests extends ESTestCase {

    private User user;
    private User userDashIndices;
    private User userNoIndices;
    private CompositeRolesStore rolesStore;
    private MetaData metaData;
    private IndicesAndAliasesResolver defaultIndicesResolver;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private Map<String, RoleDescriptor> roleMap;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 2))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
                .put("cluster.remote.remote.seeds", "127.0.0.1:" + randomIntBetween(9301, 9350))
                .put("cluster.remote.other_remote.seeds", "127.0.0.1:" + randomIntBetween(9351, 9399))
                .build();

        indexNameExpressionResolver = new IndexNameExpressionResolver();

        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_MAIN_ALIAS + (withAlias ? "-" + randomAlphaOfLength(5) : "");
        MetaData metaData = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar"))
                        .putAlias(AliasMetaData.builder("foounauthorized")).settings(settings))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar"))
                        .putAlias(AliasMetaData.builder("foobarfoo")).settings(settings))
                .put(indexBuilder("closed").state(State.CLOSE)
                        .putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("foofoo-closed").state(State.CLOSE).settings(settings))
                .put(indexBuilder("foobar-closed").state(State.CLOSE).settings(settings))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")).settings(settings))
                .put(indexBuilder("bar").settings(settings))
                .put(indexBuilder("bar-closed").state(State.CLOSE).settings(settings))
                .put(indexBuilder("bar2").settings(settings))
                .put(indexBuilder(indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>")).settings(settings))
                .put(indexBuilder("-index10").settings(settings))
                .put(indexBuilder("-index11").settings(settings))
                .put(indexBuilder("-index20").settings(settings))
                .put(indexBuilder("-index21").settings(settings))
                .put(indexBuilder("logs-00001").putAlias(AliasMetaData.builder("logs-alias").writeIndex(false)).settings(settings))
                .put(indexBuilder("logs-00002").putAlias(AliasMetaData.builder("logs-alias").writeIndex(false)).settings(settings))
                .put(indexBuilder("logs-00003").putAlias(AliasMetaData.builder("logs-alias").writeIndex(true)).settings(settings))
                .put(indexBuilder("hidden-open").settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder(".hidden-open").settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder(".hidden-closed").state(State.CLOSE)
                    .settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder("hidden-closed").state(State.CLOSE)
                    .settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder(securityIndexName).settings(settings)).build();

        if (withAlias) {
            metaData = SecurityTestUtils.addAliasToMetaData(metaData, securityIndexName);
        }
        this.metaData = metaData;

        user = new User("user", "role");
        userDashIndices = new User("dash", "dash");
        userNoIndices = new User("test", "test");
        rolesStore = mock(CompositeRolesStore.class);
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "missing", "foofoo-closed",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed"};
        String[] dashIndices = new String[]{"-index10", "-index11", "-index20", "-index21"};
        roleMap = new HashMap<>();
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(authorizedIndices).privileges("all").build() }, null));
        roleMap.put("dash", new RoleDescriptor("dash", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(dashIndices).privileges("all").build() }, null));
        roleMap.put("test", new RoleDescriptor("test", new String[] { "monitor" }, null, null));
        roleMap.put("alias_read_write", new RoleDescriptor("alias_read_write", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("barbaz", "foofoobar").privileges("read", "write").build() },
            null));
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        doAnswer((i) -> {
            ActionListener callback =
                    (ActionListener) i.getArguments()[1];
            Set<String> names = (Set<String>) i.getArguments()[0];
            assertNotNull(names);
            Set<RoleDescriptor> roleDescriptors = new HashSet<>();
            for (String name : names) {
                RoleDescriptor descriptor = roleMap.get(name);
                if (descriptor != null) {
                    roleDescriptors.add(descriptor);
                }
            }

            if (roleDescriptors.isEmpty()) {
                callback.onResponse(Role.EMPTY);
            } else {
                CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, null,
                        ActionListener.wrap(r -> callback.onResponse(r), callback::onFailure)
                );
            }
            return Void.TYPE;
        }).when(rolesStore).roles(any(Set.class), any(ActionListener.class));
        doCallRealMethod().when(rolesStore).getRoles(any(User.class), any(Authentication.class), any(ActionListener.class));

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        defaultIndicesResolver = new IndicesAndAliasesResolver(settings, clusterService);
    }

    public void testDashIndicesAreAllowedInShardLevelRequests() {
        //indices with names starting with '-' or '+' can be created up to version  2.x and can be around in 5.x
        //aliases with names starting with '-' or '+' can be created up to version 5.x and can be around in 6.x
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.indices()).thenReturn(new String[]{"-index10", "-index20", "+index30"});
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME))
                .getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index20", "+index30"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
    }

    public void testWildcardsAreNotAllowedInShardLevelRequests() {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.indices()).thenReturn(new String[]{"index*"});
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME))
                        .getLocal());
        assertEquals("There are no external requests known to support wildcards that don't support replacing their indices",
                illegalStateException.getMessage());
    }

    public void testAllIsNotAllowedInShardLevelRequests() {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        if (randomBoolean()) {
            when(request.indices()).thenReturn(new String[]{"_all"});
        } else {
            if (randomBoolean()) {
                when(request.indices()).thenReturn(Strings.EMPTY_ARRAY);
            } else {
                when(request.indices()).thenReturn(null);
            }
        }
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME))
                        .getLocal());
        assertEquals("There are no external requests known to support wildcards that don't support replacing their indices",
                illegalStateException.getMessage());
    }

    public void testExplicitDashIndices() {
        SearchRequest request = new SearchRequest("-index10", "-index20");
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index20"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testWildcardDashIndices() {
        SearchRequest request;
        if (randomBoolean()) {
            request = new SearchRequest("-index*", "--index20");
        } else {
            request = new SearchRequest("*", "--index20");
        }
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index11", "-index21"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testExplicitMixedWildcardDashIndices() {
        SearchRequest request = new SearchRequest("-index21", "-does_not_exist", "-index1*", "--index11");
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index21", "-does_not_exist"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testDashIndicesNoExpandWildcard() {
        SearchRequest request = new SearchRequest("-index1*", "--index11");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), false, false));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index1*", "--index11"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testDashIndicesMinus() {
        SearchRequest request = new SearchRequest("-index10", "-index11", "--index11", "-index20");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index11", "--index11", "-index20"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testDashIndicesPlus() {
        SearchRequest request = new SearchRequest("+bar");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)));
    }

    public void testDashNotExistingIndex() {
        SearchRequest request = new SearchRequest("-does_not_exist");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-does_not_exist"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testResolveEmptyIndicesExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveEmptyIndicesExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo"};
        assertSameValues(indices, replacedIndices);
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveAllExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveAllExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsStrictExpand() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExpandOpenAndClosedIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsStrictExpandOpen() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsLenientExpandOpen() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsMinusExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("*", "-foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foobarfoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsMinusExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("*", "-foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foobarfoo", "bar-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExclusionsExpandWilcardsOpenStrict() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "barbaz", "foob*");
        request.indicesOptions(IndicesOptions.fromOptions(false, true, true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foobarfoo", "barbaz"};
        assertSameValues(indices, replacedIndices);
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "foobarfoo", "barbaz", "foobarfoo"));
    }

    public void testResolveWildcardsPlusAndMinusExpandWilcardsOpenIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "+barbaz", "+foob*");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foobarfoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExclusionExpandWilcardsOpenAndClosedStrict() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "barbaz", "foobarfoo"};
        assertSameValues(indices, replacedIndices);
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExclusionExpandWilcardsOpenAndClosedIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foobarfoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveNonMatchingIndicesAllowNoIndices() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testResolveNonMatchingIndicesDisallowNoIndices() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index [missing*]", e.getMessage());
    }

    public void testResolveExplicitIndicesStrict() {
        SearchRequest request = new SearchRequest("missing", "bar", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"missing", "bar", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveExplicitIndicesIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("missing", "bar", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveNoAuthorizedIndicesAllowNoIndices() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        assertNoIndices(request, resolveIndices(request,
                buildAuthorizedIndices(userNoIndices, SearchAction.NAME)));
    }

    public void testResolveNoAuthorizedIndicesDisallowNoIndices() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, SearchAction.NAME)));
        assertEquals("no such index [[]]", e.getMessage());
    }

    public void testResolveMissingIndexStrict() {
        SearchRequest request = new SearchRequest("bar*", "missing");
        request.indicesOptions(IndicesOptions.fromOptions(false, true, true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"bar", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveMissingIndexIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("bar*", "missing");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNonMatchingIndicesAndExplicit() {
        SearchRequest request = new SearchRequest("missing*", "bar");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"bar"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNoExpandStrict() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), false, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"missing*"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNoExpandIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testSearchWithRemoteIndex() {
        SearchRequest request = new SearchRequest("remote:indexName");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        final ResolvedIndices resolved = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME));
        assertThat(resolved.getLocal(), emptyIterable());
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:indexName"));
        assertThat(request.indices(), arrayContaining("remote:indexName"));
    }

    public void testSearchWithRemoteAndLocalIndices() {
        SearchRequest request = new SearchRequest("remote:indexName", "bar", "bar2");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), randomBoolean(), randomBoolean()));
        final ResolvedIndices resolved = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME));
        assertThat(resolved.getLocal(), containsInAnyOrder("bar"));
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:indexName"));
        assertThat(request.indices(), arrayContainingInAnyOrder("remote:indexName", "bar"));
    }

    public void testSearchWithRemoteAndLocalWildcards() {
        SearchRequest request = new SearchRequest("*:foo", "r*:bar*", "remote:baz*", "bar*", "foofoo");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        final ResolvedIndices resolved = resolveIndices(request, authorizedIndices);
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:foo", "other_remote:foo", "remote:bar*", "remote:baz*"));
        assertThat(resolved.getLocal(), containsInAnyOrder("bar", "foofoo"));
        assertThat(request.indices(),
                arrayContainingInAnyOrder("remote:foo", "other_remote:foo", "remote:bar*", "remote:baz*", "bar", "foofoo"));
    }

    public void testResolveIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").indices("foo", "foofoo"));
        request.addAliasAction(AliasActions.add().alias("alias2").indices("foo", "foobar"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"alias1", "alias2", "foo", "foofoo", "foobar"};
        assertSameValues(indices, expectedIndices);
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("foo", "foobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveIndicesAliasesRequestExistingAlias() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").indices("foo", "foofoo"));
        request.addAliasAction(AliasActions.add().alias("foofoobar").indices("foo", "foobar"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned, foofoobar is an existing alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias1", "foofoobar", "foo", "foofoo", "foobar"};
        assertSameValues(indices, expectedIndices);
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("foo", "foobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveIndicesAliasesRequestMissingIndex() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").indices("foo", "foofoo"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("missing"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned, missing is not an existing index/alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias1", "alias2", "foo", "foofoo", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("missing"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveWildcardsIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("foo-alias").index("foo*"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("bar*"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"foo-alias", "alias2", "foofoo", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foo-alias"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveWildcardsIndicesAliasesRequestNoMatchingIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("foo*"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("bar*"));
        request.addAliasAction(AliasActions.add().alias("alias3").index("non_matching_*"));
        //if a single operation contains wildcards and ends up being resolved to no indices, it makes the whole request fail
        expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)));
    }

    public void testResolveAllIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("_all"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("_all"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoo", "alias1", "alias2"};
        assertSameValues(indices, expectedIndices);
        String[] replacedIndices = new String[]{"bar", "foofoo"};
        //_all gets replaced with all indices that user is authorized for, on each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveAllIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("_all"));
        //current user is not authorized for any index, _all resolves to no indices, the request fails
        expectThrows(IndexNotFoundException.class, () ->
                resolveIndices(request, buildAuthorizedIndices(userNoIndices, IndicesAliasesAction.NAME)));
    }

    public void testResolveWildcardsIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("foo*"));
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        expectThrows(IndexNotFoundException.class, () -> resolveIndices(
                request, buildAuthorizedIndices(userNoIndices, IndicesAliasesAction.NAME)));
    }

    public void testResolveIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo").alias("foofoobar"));
        request.addAliasAction(AliasActions.remove().index("foofoo").alias("barbaz"));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"foo", "foofoobar", "foofoo", "barbaz"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("barbaz"));
    }

    public void testResolveIndicesAliasesRequestDeleteActionsMissingIndex() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo").alias("foofoobar"));
        request.addAliasAction(AliasActions.remove().index("missing_index").alias("missing_alias"));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned, doesn't matter is some of them don't exist
        String[] expectedIndices = new String[]{"foo", "foofoobar", "missing_index", "missing_alias"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("missing_index"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("missing_alias"));
    }

    public void testResolveWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo*").alias("foofoobar"));
        request.addAliasAction(AliasActions.remove().index("bar*").alias("barbaz"));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        String[] expectedIndices = new String[]{"foofoobar", "foofoo", "bar", "barbaz"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced within each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("barbaz"));
    }

    public void testResolveAliasesWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("*").alias("foo*"));
        request.addAliasAction(AliasActions.remove().index("*bar").alias("foo*"));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("bar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo"));
    }

    public void testResolveAllAliasesWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("*").alias("_all"));
        request.addAliasAction(AliasActions.remove().index("_all").aliases("_all", "explicit"));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo", "explicit"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("bar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo"));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("bar", "foofoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo", "explicit"));
    }

    public void testResolveAliasesWildcardsIndicesAliasesRequestRemoveAliasActionsNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo*").alias("foo*"));
        request.addAliasAction(AliasActions.remove().index("*bar").alias("bar*"));
        resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContaining("*", "-*"));
    }

    public void testResolveAliasesWildcardsIndicesAliasesRequestRemoveIndexActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.removeIndex().index("foo*"));
        request.addAliasAction(AliasActions.removeIndex().index("*bar"));
        resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), emptyArray());
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), emptyArray());
    }

    public void testResolveWildcardsIndicesAliasesRequestAddAndDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo*").alias("foofoobar"));
        request.addAliasAction(AliasActions.add().index("bar*").alias("foofoobar"));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        String[] expectedIndices = new String[]{"foofoobar", "foofoo", "bar"};
        assertSameValues(indices, expectedIndices);
        //every single action has its indices replaced with matching (authorized) ones
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveGetAliasesRequestStrict() {
        GetAliasesRequest request = new GetAliasesRequest("alias1").indices("foo", "foofoo");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"alias1", "foo", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveGetAliasesRequestIgnoreUnavailable() {
        GetAliasesRequest request = new GetAliasesRequest("alias1").indices("foo", "foofoo");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), randomBoolean(), randomBoolean()));
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        String[] expectedIndices = new String[]{"alias1", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveGetAliasesRequestMissingIndexStrict() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned, missing is not an existing index/alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias2", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("missing"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testGetAliasesRequestMissingIndexIgnoreUnavailableDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        IndexNotFoundException exception = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)).getLocal());
        assertEquals("no such index [[missing]]", exception.getMessage());
    }

    public void testGetAliasesRequestMissingIndexIgnoreUnavailableAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(true, true, randomBoolean(), randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)));
    }

    public void testGetAliasesRequestMissingIndexStrict() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        String[] expectedIndices = new String[]{"alias2", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("missing"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveWildcardsGetAliasesRequestStrictExpand() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, true));
        request.aliases("alias1");
        request.indices("foo*");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoo-closed", "foofoobar", "foobarfoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoobar", "foobarfoo", "foofoo", "foofoo-closed"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveWildcardsGetAliasesRequestStrictExpandOpen() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, false));
        request.aliases("alias1");
        request.indices("foo*");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoobar", "foobarfoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoobar", "foobarfoo", "foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveWildcardsGetAliasesRequestLenientExpandOpen() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, false));
        request.aliases("alias1");
        request.indices("foo*", "bar", "missing");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoobar", "foobarfoo", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoobar", "foobarfoo", "foofoo", "bar"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testWildcardsGetAliasesRequestNoMatchingIndicesDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        request.aliases("alias3");
        request.indices("non_matching_*");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)).getLocal());
        assertEquals("no such index [non_matching_*]", e.getMessage());
    }

    public void testWildcardsGetAliasesRequestNoMatchingIndicesAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        request.aliases("alias3");
        request.indices("non_matching_*");
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)));
    }

    public void testResolveAllGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        //even if not set, empty means _all
        if (randomBoolean()) {
            request.indices("_all");
        }
        request.aliases("alias1");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "alias1"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed"};
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveAllGetAliasesRequestExpandWildcardsOpenOnly() {
        GetAliasesRequest request = new GetAliasesRequest();
        //set indices options to have wildcards resolved to open indices only (default is open and closed)
        request.indicesOptions(IndicesOptions.fromOptions(true, false, true, false));
        //even if not set, empty means _all
        if (randomBoolean()) {
            request.indices("_all");
        }
        request.aliases("alias1");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo", "alias1"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo"};
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testAllGetAliasesRequestNoAuthorizedIndicesAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        request.aliases("alias1");
        request.indices("_all");
        assertNoIndices(request, resolveIndices(request,
                buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
    }

    public void testAllGetAliasesRequestNoAuthorizedIndicesDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        request.aliases("alias1");
        request.indices("_all");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
        assertEquals("no such index [[_all]]", e.getMessage());
    }

    public void testWildcardsGetAliasesRequestNoAuthorizedIndicesAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("alias1");
        request.indices("foo*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        assertNoIndices(request, resolveIndices(request,
                buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
    }

    public void testWildcardsGetAliasesRequestNoAuthorizedIndicesDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        request.aliases("alias1");
        request.indices("foo*");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
        assertEquals("no such index [foo*]", e.getMessage());
    }

    public void testResolveAllAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        if (randomBoolean()) {
            request.aliases("_all");
        }
        if (randomBoolean()) {
            request.indices("_all");
        }
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed"};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo"));
    }

    public void testResolveAllAndExplicitAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "explicit"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "explicit"};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo", "explicit"));
    }

    public void testResolveAllAndWildcardsAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "foo*", "non_matching_*"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed"};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foofoobar", "foobarfoo", "foobarfoo"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("*bar");
        request.aliases("foo*");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foobarfoo", "foofoobar"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "foofoobar"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        //no authorized aliases match bar*, hence aliases are replaced with the no-aliases-expression
        request.aliases("bar*");
        request.indices("*bar");
        resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME));
        assertThat(request.aliases(), arrayContaining(IndicesAndAliasesResolver.NO_INDICES_OR_ALIASES_ARRAY));
    }

    public void testResolveAliasesExclusionWildcardsGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("foo*","-foobar*");
        final List<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foobarfoo", "foofoo", "foofoo-closed", "foofoobar"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "bar-closed", "foobarfoo", "foofoo", "foofoo-closed", "foofoobar"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveAliasesAllGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        if (randomBoolean()) {
            request.aliases("_all");
        }
        request.indices("non_existing");
        //current user is not authorized for any index, aliases are replaced with the no-aliases-expression
        ResolvedIndices resolvedIndices = resolveIndices(request, buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME));
        assertThat(resolvedIndices.getLocal(), contains("non_existing"));
        assertThat(Arrays.asList(request.indices()), contains("non_existing"));
        assertThat(request.aliases(), arrayContaining(IndicesAndAliasesResolver.NO_INDICES_OR_ALIASES_ARRAY));
    }

    /**
     * Tests that all the request types that are known to support remote indices successfully pass them through
     *  the resolver
     */
    public void testRemotableRequestsAllowRemoteIndices() {
        IndicesOptions options = IndicesOptions.fromOptions(true, false, false, false);
        Tuple<TransportRequest, String> tuple = randomFrom(
                new Tuple<TransportRequest, String>(new SearchRequest("remote:foo").indicesOptions(options), SearchAction.NAME),
                new Tuple<TransportRequest, String>(new FieldCapabilitiesRequest().indices("remote:foo").indicesOptions(options),
                        FieldCapabilitiesAction.NAME),
                new Tuple<TransportRequest, String>(new GraphExploreRequest().indices("remote:foo").indicesOptions(options),
                        GraphExploreAction.NAME)
        );
        final TransportRequest request = tuple.v1();
        ResolvedIndices resolved = resolveIndices(request, buildAuthorizedIndices(user, tuple.v2()));
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:foo"));
        assertThat(resolved.getLocal(), emptyIterable());
        assertThat(((IndicesRequest) request).indices(), arrayContaining("remote:foo"));
    }

    /**
     * Tests that request types that do not support remote indices will be resolved as if all index names are local.
     */
    public void testNonRemotableRequestDoesNotAllowRemoteIndices() {
        IndicesOptions options = IndicesOptions.fromOptions(true, false, false, false);
        Tuple<TransportRequest, String> tuple = randomFrom(
                new Tuple<TransportRequest, String>(new CloseIndexRequest("remote:foo").indicesOptions(options), CloseIndexAction.NAME),
                new Tuple<TransportRequest, String>(new DeleteIndexRequest("remote:foo").indicesOptions(options), DeleteIndexAction.NAME),
                new Tuple<TransportRequest, String>(new PutMappingRequest("remote:foo").indicesOptions(options), PutMappingAction.NAME)
        );
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(tuple.v1(), buildAuthorizedIndices(user, tuple.v2())).getLocal());
        assertEquals("no such index [[remote:foo]]", e.getMessage());
    }

    public void testNonRemotableRequestDoesNotAllowRemoteWildcardIndices() {
        IndicesOptions options = IndicesOptions.fromOptions(randomBoolean(), true, true, true);
        Tuple<TransportRequest, String> tuple = randomFrom(
                new Tuple<TransportRequest, String>(new CloseIndexRequest("*:*").indicesOptions(options), CloseIndexAction.NAME),
                new Tuple<TransportRequest, String>(new DeleteIndexRequest("*:*").indicesOptions(options), DeleteIndexAction.NAME),
                new Tuple<TransportRequest, String>(new PutMappingRequest("*:*").indicesOptions(options), PutMappingAction.NAME)
        );
        final ResolvedIndices resolved = resolveIndices(tuple.v1(), buildAuthorizedIndices(user, tuple.v2()));
        assertNoIndices((IndicesRequest.Replaceable) tuple.v1(), resolved);
    }

    public void testCompositeIndicesRequestIsNotSupported() {
        TransportRequest request = randomFrom(new MultiSearchRequest(), new MultiGetRequest(),
                new MultiTermVectorsRequest(), new BulkRequest());
        expectThrows(IllegalStateException.class, () -> resolveIndices(request,
                buildAuthorizedIndices(user, MultiSearchAction.NAME)));
    }

    public void testResolveAdminAction() {
        final List<String> authorizedIndices = buildAuthorizedIndices(user, DeleteIndexAction.NAME);
        {
            RefreshRequest request = new RefreshRequest("*");
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            String[] expectedIndices = new String[]{"bar", "foofoobar", "foobarfoo", "foofoo"};
            assertThat(indices.size(), equalTo(expectedIndices.length));
            assertThat(indices, hasItems(expectedIndices));
            assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        }
        {
            DeleteIndexRequest request = new DeleteIndexRequest("*");
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoo", "foofoo-closed"};
            assertThat(indices.size(), equalTo(expectedIndices.length));
            assertThat(indices, hasItems(expectedIndices));
            assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        }
    }

    public void testXPackSecurityUserHasAccessToSecurityIndex() {
        SearchRequest request = new SearchRequest();
        {
            final List<String> authorizedIndices = buildAuthorizedIndices(XPackSecurityUser.INSTANCE, SearchAction.NAME);
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            assertThat(indices, hasItem(SECURITY_MAIN_ALIAS));
        }
        {
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            aliasesRequest.addAliasAction(AliasActions.add().alias("security_alias").index(SECURITY_MAIN_ALIAS));
            final List<String> authorizedIndices = buildAuthorizedIndices(XPackSecurityUser.INSTANCE, IndicesAliasesAction.NAME);
            List<String> indices = resolveIndices(aliasesRequest, authorizedIndices).getLocal();
            assertThat(indices, hasItem(SECURITY_MAIN_ALIAS));
        }
    }

    public void testXPackUserDoesNotHaveAccessToSecurityIndex() {
        SearchRequest request = new SearchRequest();
        final List<String> authorizedIndices = buildAuthorizedIndices(XPackUser.INSTANCE, SearchAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        assertThat(indices, not(hasItem(SECURITY_MAIN_ALIAS)));
    }

    public void testNonXPackUserAccessingSecurityIndex() {
        User allAccessUser = new User("all_access", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all_access", new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));

        {
            SearchRequest request = new SearchRequest();
            final List<String> authorizedIndices = buildAuthorizedIndices(allAccessUser, SearchAction.NAME);
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            assertThat(indices, not(hasItem(SECURITY_MAIN_ALIAS)));
        }

        {
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            aliasesRequest.addAliasAction(AliasActions.add().alias("security_alias1").index("*"));
            final List<String> authorizedIndices = buildAuthorizedIndices(allAccessUser, IndicesAliasesAction.NAME);
            List<String> indices = resolveIndices(aliasesRequest, authorizedIndices).getLocal();
            assertThat(indices, not(hasItem(SECURITY_MAIN_ALIAS)));
        }
    }

    public void testUnauthorizedDateMathExpressionIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("<datetime-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, randomBoolean(), randomBoolean()));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testUnauthorizedDateMathExpressionIgnoreUnavailableDisallowNoIndices() {
        SearchRequest request = new SearchRequest("<datetime-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index [[<datetime-{now/M}>]]" , e.getMessage());
    }

    public void testUnauthorizedDateMathExpressionStrict() {
        String expectedIndex = "datetime-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(
            new DateTime(DateTimeZone.UTC).monthOfYear().roundFloorCopy());
        SearchRequest request = new SearchRequest("<datetime-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index [" + expectedIndex + "]" , e.getMessage());
    }

    public void testResolveDateMathExpression() {
        // make the user authorized
        final String pattern = randomBoolean() ? "<datetime-{now/M}>" : "<datetime-{now/M}*>";
        String dateTimeIndex = indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>");
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed", dateTimeIndex};
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(authorizedIndices).privileges("all").build() }, null));

        SearchRequest request = new SearchRequest(pattern);
        if (randomBoolean()) {
            final boolean expandIndicesOpen = Regex.isSimpleMatchPattern(pattern) ? true : randomBoolean();
            request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), expandIndicesOpen, randomBoolean()));
        }
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        assertThat(indices.size(), equalTo(1));
        assertThat(request.indices()[0], equalTo(dateTimeIndex));
    }

    public void testMissingDateMathExpressionIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("<foobar-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, randomBoolean(), randomBoolean()));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testMissingDateMathExpressionIgnoreUnavailableDisallowNoIndices() {
        SearchRequest request = new SearchRequest("<foobar-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index [[<foobar-{now/M}>]]" , e.getMessage());
    }

    public void testMissingDateMathExpressionStrict() {
        String expectedIndex = "foobar-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(
            new DateTime(DateTimeZone.UTC).monthOfYear().roundFloorCopy());
        SearchRequest request = new SearchRequest("<foobar-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index [" + expectedIndex + "]" , e.getMessage());
    }

    public void testAliasDateMathExpressionNotSupported() {
        // make the user authorized
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed",
                indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>")};
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(authorizedIndices).privileges("all").build() }, null));
        GetAliasesRequest request = new GetAliasesRequest("<datetime-{now/M}>").indices("foo", "foofoo");
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"<datetime-{now/M}>", "foo", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("<datetime-{now/M}>"));
    }

    public void testDynamicPutMappingRequestFromAlias() {
        PutMappingRequest request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index("foofoo", UUIDs.base64UUID()));
        User user = new User("alias-writer", "alias_read_write");
        List<String> authorizedIndices = buildAuthorizedIndices(user, PutMappingAction.NAME);

        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metaData);
        assertEquals("barbaz", putMappingIndexOrAlias);

        // multiple indices map to an alias so we can only return the concrete index
        final String index = randomFrom("foo", "foobar");
        request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metaData);
        assertEquals(index, putMappingIndexOrAlias);

    }

    public void testWhenAliasToMultipleIndicesAndUserIsAuthorizedUsingAliasReturnsAliasNameForDynamicPutMappingRequestOnWriteIndex() {
        String index = "logs-00003"; // write index
        PutMappingRequest request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        List<String> authorizedIndices = Collections.singletonList("logs-alias");
        assert metaData.getAliasAndIndexLookup().get("logs-alias").getIndices().size() == 3;
        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metaData);
        String message = "user is authorized to access `logs-alias` and the put mapping request is for a write index"
                + "so this should have returned the alias name";
        assertEquals(message, "logs-alias", putMappingIndexOrAlias);
    }

    public void testWhenAliasToMultipleIndicesAndUserIsAuthorizedUsingAliasReturnsIndexNameForDynamicPutMappingRequestOnReadIndex() {
        String index = "logs-00002"; // read index
        PutMappingRequest request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        List<String> authorizedIndices = Collections.singletonList("logs-alias");
        assert metaData.getAliasAndIndexLookup().get("logs-alias").getIndices().size() == 3;
        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metaData);
        String message = "user is authorized to access `logs-alias` and the put mapping request is for a read index"
                + "so this should have returned the concrete index as fallback";
        assertEquals(message, index, putMappingIndexOrAlias);
    }

    public void testHiddenIndicesResolution() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, true, true));
        List<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(searchRequest, metaData, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // open + hidden
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, true));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(searchRequest, metaData, authorizedIndices);
        assertThat(resolvedIndices.getLocal(),
            containsInAnyOrder("bar", "foofoobar", "foobarfoo", "foofoo", "hidden-open", ".hidden-open"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // open + implicit hidden for . indices
        searchRequest = new SearchRequest(randomFrom(".*", ".hid*"));
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(searchRequest, metaData, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder(".hidden-open"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // closed + hidden, ignore aliases
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, false, true, true, true, false, true, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(searchRequest, metaData, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("bar-closed", "foofoo-closed", "hidden-closed", ".hidden-closed"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // closed + implicit hidden for . indices
        searchRequest = new SearchRequest(randomFrom(".*", ".hid*"));
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, false, true, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(searchRequest, metaData, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder(".hidden-closed"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // allow no indices, do not expand to open or closed, expand hidden, ignore aliases
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, true, false, false, false, true, false, true, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(searchRequest, metaData, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), contains("-*"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());
    }

    private List<String> buildAuthorizedIndices(User user, String action) {
        PlainActionFuture<Role> rolesListener = new PlainActionFuture<>();
        final Authentication authentication =
            new Authentication(user, new RealmRef("test", "indices-aliases-resolver-tests", "node"), null);
        rolesStore.getRoles(user, authentication, rolesListener);
        return RBACEngine.resolveAuthorizedIndicesFromRole(rolesListener.actionGet(), action, metaData.getAliasAndIndexLookup());
    }

    public static IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    private ResolvedIndices resolveIndices(TransportRequest request, List<String> authorizedIndices) {
        return defaultIndicesResolver.resolve(request, this.metaData, authorizedIndices);
    }

    private static void assertNoIndices(IndicesRequest.Replaceable request, ResolvedIndices resolvedIndices) {
        final List<String> localIndices = resolvedIndices.getLocal();
        assertEquals(1, localIndices.size());
        assertEquals(IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER, localIndices.iterator().next());
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_OR_ALIASES_LIST, Arrays.asList(request.indices()));
        assertEquals(0, resolvedIndices.getRemote().size());
    }

    private void assertSameValues(List<String> indices, String[] expectedIndices) {
        assertThat(indices.stream().distinct().count(), equalTo((long)expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
    }
}
