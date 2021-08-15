/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
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
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.authz.AuthorizedIndicesTests.getRequestInfo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
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
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesAndAliasesResolverTests extends ESTestCase {

    private User user;
    private User userDashIndices;
    private User userNoIndices;
    private CompositeRolesStore rolesStore;
    private Metadata metadata;
    private IndicesAndAliasesResolver defaultIndicesResolver;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private Map<String, RoleDescriptor> roleMap;
    private String todaySuffix;
    private String tomorrowSuffix;

    @Before
//    @SuppressWarnings("unchecked")
    public void setup() {
        Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 2))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
                .put("cluster.remote.remote.seeds", "127.0.0.1:" + randomIntBetween(9301, 9350))
                .put("cluster.remote.other_remote.seeds", "127.0.0.1:" + randomIntBetween(9351, 9399))
                .build();

        indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();

        DateFormatter dateFormatter = DateFormatter.forPattern("uuuu.MM.dd");
        Instant now = Instant.now(Clock.systemUTC());
        todaySuffix = dateFormatter.format(now);
        tomorrowSuffix = dateFormatter.format(now.plus(Duration.ofDays(1L)));
        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_MAIN_ALIAS + (withAlias ? "-" + randomAlphaOfLength(5) : "");
        final String dataStreamName = "logs-foobar";
        final String otherDataStreamName = "logs-foo";
        IndexMetadata dataStreamIndex1 = DataStreamTestHelper.createBackingIndex(dataStreamName, 1).build();
        IndexMetadata dataStreamIndex2 = DataStreamTestHelper.createBackingIndex(dataStreamName, 2).build();
        IndexMetadata dataStreamIndex3 = DataStreamTestHelper.createBackingIndex(otherDataStreamName, 1).build();
        Metadata metadata = Metadata.builder()
                .put(indexBuilder("foo").putAlias(AliasMetadata.builder("foofoobar"))
                        .putAlias(AliasMetadata.builder("foounauthorized")).settings(settings))
                .put(indexBuilder("foobar").putAlias(AliasMetadata.builder("foofoobar"))
                        .putAlias(AliasMetadata.builder("foobarfoo")).settings(settings))
                .put(indexBuilder("closed").state(State.CLOSE)
                        .putAlias(AliasMetadata.builder("foofoobar")).settings(settings))
                .put(indexBuilder("foofoo-closed").state(State.CLOSE).settings(settings))
                .put(indexBuilder("foobar-closed").state(State.CLOSE).settings(settings))
                .put(indexBuilder("foofoo").putAlias(AliasMetadata.builder("barbaz")).settings(settings))
                .put(indexBuilder("bar").settings(settings))
                .put(indexBuilder("bar-closed").state(State.CLOSE).settings(settings))
                .put(indexBuilder("bar2").settings(settings))
                .put(indexBuilder(indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>")).settings(settings))
                .put(indexBuilder("-index10").settings(settings))
                .put(indexBuilder("-index11").settings(settings))
                .put(indexBuilder("-index20").settings(settings))
                .put(indexBuilder("-index21").settings(settings))
                .put(indexBuilder("logs-00001").putAlias(AliasMetadata.builder("logs-alias").writeIndex(false)).settings(settings))
                .put(indexBuilder("logs-00002").putAlias(AliasMetadata.builder("logs-alias").writeIndex(false)).settings(settings))
                .put(indexBuilder("logs-00003").putAlias(AliasMetadata.builder("logs-alias").writeIndex(true)).settings(settings))
                .put(indexBuilder("hidden-open").settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder(".hidden-open").settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder(".hidden-closed").state(State.CLOSE)
                    .settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder("hidden-closed").state(State.CLOSE)
                    .settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder("hidden-w-aliases").settings(Settings.builder().put(settings).put("index.hidden", true).build())
                    .putAlias(AliasMetadata.builder("alias-hidden").isHidden(true).build())
                    .putAlias(AliasMetadata.builder(".alias-hidden").isHidden(true).build())
                    .putAlias(AliasMetadata.builder("alias-visible-mixed").isHidden(false).build()))
                .put(indexBuilder("hidden-w-visible-alias").settings(Settings.builder().put(settings).put("index.hidden", true).build())
                    .putAlias(AliasMetadata.builder("alias-visible").build()))
                .put(indexBuilder("visible-w-aliases").settings(Settings.builder().put(settings).build())
                    .putAlias(AliasMetadata.builder("alias-visible").build())
                    .putAlias(AliasMetadata.builder("alias-visible-mixed").isHidden(false).build()))
                .put(indexBuilder("date-hidden-" + todaySuffix)
                    .settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(indexBuilder("date-hidden-" + tomorrowSuffix)
                    .settings(Settings.builder().put(settings).put("index.hidden", true).build()))
                .put(dataStreamIndex1, true)
                .put(dataStreamIndex2, true)
                .put(dataStreamIndex3, true)
                .put(new DataStream(dataStreamName, createTimestampField("@timestamp"),
                    Arrays.asList(dataStreamIndex1.getIndex(), dataStreamIndex2.getIndex())))
                .put(new DataStream(otherDataStreamName, createTimestampField("@timestamp"),
                    Arrays.asList(dataStreamIndex3.getIndex())))
                .put(indexBuilder(securityIndexName).settings(settings)).build();

        if (withAlias) {
            metadata = SecurityTestUtils.addAliasToMetadata(metadata, securityIndexName);
        }
        this.metadata = metadata;

        user = new User("user", "role");
        userDashIndices = new User("dash", "dash");
        userNoIndices = new User("test", "test");
        rolesStore = mock(CompositeRolesStore.class);
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "missing", "foofoo-closed",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix,
            "date-hidden-" + tomorrowSuffix};
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
        roleMap.put("hidden_alias_test", new RoleDescriptor("hidden_alias_test", null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("alias-visible", "alias-visible-mixed", "alias-hidden", ".alias-hidden", "hidden-open")
                    .privileges("all")
                    .build()
            }, null));
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        roleMap.put("data_stream_test1", new RoleDescriptor("data_stream_test1", null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices(dataStreamName + "*")
                    .privileges("all")
                    .build()
            }, null));
        roleMap.put("data_stream_test2", new RoleDescriptor("data_stream_test2", null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices(otherDataStreamName + "*")
                    .privileges("all")
                    .build()
            }, null));
        roleMap.put("data_stream_test3", new RoleDescriptor("data_stream_test3", null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("logs*")
                    .privileges("all")
                    .build()
            }, null));
        roleMap.put("backing_index_test_wildcards", new RoleDescriptor("backing_index_test_wildcards", null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices(".ds-logs*")
                    .privileges("all")
                    .build()
            }, null));
        roleMap.put("backing_index_test_name", new RoleDescriptor("backing_index_test_name", null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices(dataStreamIndex1.getIndex().getName())
                    .privileges("all")
                    .build()
            }, null));
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Role> callback = (ActionListener<Role>) i.getArguments()[1];
            @SuppressWarnings("unchecked")
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
        }).when(rolesStore).roles(anySetOf(String.class), anyActionListener());
        doCallRealMethod().when(rolesStore).getRoles(any(User.class), any(Authentication.class), anyActionListener());

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        defaultIndicesResolver =
            new IndicesAndAliasesResolver(settings, clusterService, indexNameExpressionResolver);
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
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> resolveIndices(SearchAction.NAME + "[s]", request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal()
        );
        assertThat(
            exception,
            throwableWithMessage(
                "the action indices:data/read/search[s] does not support wildcards;"
                    + " the provided index expression(s) [index*] are not allowed"
            )
        );
    }

    public void testAllIsNotAllowedInShardLevelRequests() {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        final boolean literalAll = randomBoolean();
        if (literalAll) {
            when(request.indices()).thenReturn(new String[]{"_all"});
        } else {
            if (randomBoolean()) {
                when(request.indices()).thenReturn(Strings.EMPTY_ARRAY);
            } else {
                when(request.indices()).thenReturn(null);
            }
        }
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> resolveIndices(SearchAction.NAME + "[s]", request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal()
        );

        assertThat(
            exception,
            literalAll
                ? throwableWithMessage(
                    "the action indices:data/read/search[s] does not support accessing all indices;"
                        + " the provided index expression [_all] is not allowed"
                )
                : throwableWithMessage("the action indices:data/read/search[s] requires explicit index names, but none were provided")
        );
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, including hidden indices as Get Aliases includes hidden by default
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "alias1",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix,
            "date-hidden-" + tomorrowSuffix};
        assertSameValues(indices, expectedIndices);
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "hidden-open",
            "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix, "date-hidden-" + tomorrowSuffix};
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, including hidden indices as Get Aliases includes hidden by default
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "hidden-open",
            "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix, "date-hidden-" + tomorrowSuffix};
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, including hidden indices as Get Aliases includes hidden by default
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "explicit",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix,
            "date-hidden-" + tomorrowSuffix};
        logger.info("indices: {}", indices);
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix,
            "date-hidden-" + tomorrowSuffix));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foobarfoo", "explicit"));
    }

    public void testResolveAllAndWildcardsAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "foo*", "non_matching_*"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, including hidden indices as Get Aliases includes hidden by default
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed", "hidden-open",
            "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix, "date-hidden-" + tomorrowSuffix};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foofoobar", "foobarfoo", "foobarfoo"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("*bar");
        request.aliases("foo*");
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        //also includes hidden indices as Get Aliases includes hidden by default
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foobarfoo", "foofoo", "foofoo-closed", "foofoobar", "hidden-open",
            "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix, "date-hidden-" + tomorrowSuffix};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "bar-closed", "foobarfoo", "foofoo", "foofoo-closed", "foofoobar",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix,
            "date-hidden-" + tomorrowSuffix));
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
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, DeleteIndexAction.NAME);
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

    public void testIndicesExists() {
        //verify that the ignore_unavailable and allow_no_indices get replaced like es core does, to make sure that
        //indices exists api never throws exception due to missing indices, but only returns false instead.
        {
            IndicesExistsRequest request = new IndicesExistsRequest();
            assertNoIndices(request, resolveIndices(request,
                    buildAuthorizedIndices(userNoIndices, IndicesExistsAction.NAME)));
        }

        {
            IndicesExistsRequest request = new IndicesExistsRequest("does_not_exist");

            assertNoIndices(request, resolveIndices(request,
                    buildAuthorizedIndices(user, IndicesExistsAction.NAME)));
        }
        {
            IndicesExistsRequest request = new IndicesExistsRequest("does_not_exist_*");
            assertNoIndices(request, resolveIndices(request,
                    buildAuthorizedIndices(user, IndicesExistsAction.NAME)));
        }
    }

    public void testXPackSecurityUserHasAccessToSecurityIndex() {
        SearchRequest request = new SearchRequest();
        {
            final Set<String> authorizedIndices = buildAuthorizedIndices(XPackSecurityUser.INSTANCE, SearchAction.NAME);
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            assertThat(indices, hasItem(SECURITY_MAIN_ALIAS));
        }
        {
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            aliasesRequest.addAliasAction(AliasActions.add().alias("security_alias").index(SECURITY_MAIN_ALIAS));
            final Set<String> authorizedIndices = buildAuthorizedIndices(XPackSecurityUser.INSTANCE, IndicesAliasesAction.NAME);
            List<String> indices = resolveIndices(aliasesRequest, authorizedIndices).getLocal();
            assertThat(indices, hasItem(SECURITY_MAIN_ALIAS));
        }
    }

    public void testXPackUserDoesNotHaveAccessToSecurityIndex() {
        SearchRequest request = new SearchRequest();
        final Set<String> authorizedIndices = buildAuthorizedIndices(XPackUser.INSTANCE, SearchAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        assertThat(indices, not(hasItem(SECURITY_MAIN_ALIAS)));
    }

    public void testNonXPackUserAccessingSecurityIndex() {
        User allAccessUser = new User("all_access", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all_access", new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));

        {
            SearchRequest request = new SearchRequest();
            final Set<String> authorizedIndices = buildAuthorizedIndices(allAccessUser, SearchAction.NAME);
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            assertThat(indices, not(hasItem(SECURITY_MAIN_ALIAS)));
        }

        {
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            aliasesRequest.addAliasAction(AliasActions.add().alias("security_alias1").index("*"));
            final Set<String> authorizedIndices = buildAuthorizedIndices(allAccessUser, IndicesAliasesAction.NAME);
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
        Set<String> authorizedIndices = buildAuthorizedIndices(user, PutMappingAction.NAME);

        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metadata);
        assertEquals("barbaz", putMappingIndexOrAlias);

        // multiple indices map to an alias so we can only return the concrete index
        final String index = randomFrom("foo", "foobar");
        request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metadata);
        assertEquals(index, putMappingIndexOrAlias);

    }

    public void testWhenAliasToMultipleIndicesAndUserIsAuthorizedUsingAliasReturnsAliasNameForDynamicPutMappingRequestOnWriteIndex() {
        String index = "logs-00003"; // write index
        PutMappingRequest request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        Set<String> authorizedIndices = Collections.singleton("logs-alias");
        assert metadata.getIndicesLookup().get("logs-alias").getIndices().size() == 3;
        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metadata);
        String message = "user is authorized to access `logs-alias` and the put mapping request is for a write index"
                + "so this should have returned the alias name";
        assertEquals(message, "logs-alias", putMappingIndexOrAlias);
    }

    public void testWhenAliasToMultipleIndicesAndUserIsAuthorizedUsingAliasReturnsIndexNameForDynamicPutMappingRequestOnReadIndex() {
        String index = "logs-00002"; // read index
        PutMappingRequest request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        Set<String> authorizedIndices = Collections.singleton("logs-alias");
        assert metadata.getIndicesLookup().get("logs-alias").getIndices().size() == 3;
        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metadata);
        String message = "user is authorized to access `logs-alias` and the put mapping request is for a read index"
                + "so this should have returned the concrete index as fallback";
        assertEquals(message, index, putMappingIndexOrAlias);
    }

    public void testHiddenIndicesResolution() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, true, true));
        Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        ResolvedIndices resolvedIndices
            = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("bar", "bar-closed", "foofoobar", "foobarfoo", "foofoo", "foofoo-closed",
            "hidden-open", "hidden-closed", ".hidden-open", ".hidden-closed", "date-hidden-" + todaySuffix,
            "date-hidden-" + tomorrowSuffix));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // open + hidden
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, true));
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(),
            containsInAnyOrder("bar", "foofoobar", "foobarfoo", "foofoo", "hidden-open", ".hidden-open", "date-hidden-" + todaySuffix,
                "date-hidden-" + tomorrowSuffix));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // open + implicit hidden for . indices
        searchRequest = new SearchRequest(randomFrom(".h*", ".hid*"));
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder(".hidden-open"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // closed + hidden, ignore aliases
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, false, true, true, true, false, true, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("bar-closed", "foofoo-closed", "hidden-closed", ".hidden-closed"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // closed + implicit hidden for . indices
        searchRequest = new SearchRequest(randomFrom(".h*", ".hid*"));
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, false, true, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder(".hidden-closed"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // allow no indices, do not expand to open or closed, expand hidden, ignore aliases
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, true, false, false, false, true, false, true, false));
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), contains("-*"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // date math with default indices options
        searchRequest = new SearchRequest("<date-hidden-{now/d}>");
        authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), contains(oneOf("date-hidden-" + todaySuffix, "date-hidden-" + tomorrowSuffix)));
        assertThat(resolvedIndices.getRemote(), emptyIterable());
    }

    public void testHiddenAliasesResolution() {
        final User user = new User("hidden-alias-tester", "hidden_alias_test");
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);

        // Visible only
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false));
        ResolvedIndices resolvedIndices
            = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("alias-visible", "alias-visible-mixed"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // Include hidden explicitly
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, true));
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(),
            containsInAnyOrder("alias-visible", "alias-visible-mixed", "alias-hidden", ".alias-hidden", "hidden-open"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // Include hidden with a wildcard
        searchRequest = new SearchRequest("alias-h*");
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, true));
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("alias-hidden"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // Dot prefix, implicitly including hidden
        searchRequest = new SearchRequest(".a*");
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false));
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder(".alias-hidden"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // Make sure ignoring aliases works (visible only)
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, true, true, false, false, true, false, true, false));
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), contains("-*"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());

        // Make sure ignoring aliases works (including hidden)
        searchRequest = new SearchRequest();
        searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, true, true, false, true, false));
        resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(SearchAction.NAME, searchRequest, metadata, authorizedIndices);
        assertThat(resolvedIndices.getLocal(), containsInAnyOrder("hidden-open"));
        assertThat(resolvedIndices.getRemote(), emptyIterable());
    }

    public void testDataStreamResolution() {
        {
            final User user = new User("data-stream-tester1", "data_stream_test1");

            // Resolve data streams:
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("logs-*");
            searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false, true, true, true, true));
            final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME, searchRequest);
            ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
                SearchAction.NAME,
                searchRequest,
                metadata,
                authorizedIndices
            );
            assertThat(resolvedIndices.getLocal(), contains("logs-foobar"));
            assertThat(resolvedIndices.getRemote(), emptyIterable());

            // Data streams with allow no indices:
            searchRequest = new SearchRequest();
            searchRequest.indices("logs-*");
            searchRequest.indicesOptions(IndicesOptions.fromOptions(false, true, true, false, false, true, true, true, true));
            resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
                SearchAction.NAME,
                searchRequest,
                metadata,
                authorizedIndices
            );
            // if data streams are to be ignored then this happens in IndexNameExpressionResolver:
            assertThat(resolvedIndices.getLocal(), contains("logs-foobar"));
            assertThat(resolvedIndices.getRemote(), emptyIterable());
        }
        {
            final User user = new User("data-stream-tester2", "data_stream_test2");

            // Resolve *all* data streams:
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("logs-*");
            searchRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, false, false, true, true, true, true));
            final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME, searchRequest);
            ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
                SearchAction.NAME,
                searchRequest,
                metadata,
                authorizedIndices
            );
            assertThat(resolvedIndices.getLocal(), containsInAnyOrder("logs-foo", "logs-foobar"));
            assertThat(resolvedIndices.getRemote(), emptyIterable());
        }
    }

    public void testDataStreamsAreNotVisibleWhenNotIncludedByRequestWithWildcard() {
        final User user = new User("data-stream-tester2", "data_stream_test2");
        GetAliasesRequest request = new GetAliasesRequest("*");
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams and their backing indices should _not_ be in the authorized list since the backing indices
        // do not match the requested pattern
        List<String> dataStreams = org.elasticsearch.core.List.of("logs-foo", "logs-foobar");
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        for (String dsName : dataStreams) {
            assertThat(authorizedIndices, hasItem(dsName));
            DataStream dataStream = metadata.dataStreams().get(dsName);
            assertThat(authorizedIndices, hasItem(dsName));
            for (Index i : dataStream.getIndices()) {
                assertThat(authorizedIndices, hasItem(i.getName()));
            }
        }

        // neither data streams nor their backing indices will be in the resolved list unless the backing indices matched the requested
        // pattern
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            GetAliasesAction.NAME,
            request,
            metadata,
            authorizedIndices);
        for (String dsName : dataStreams) {
            assertThat(resolvedIndices.getLocal(), hasItem(dsName));
            DataStream dataStream = metadata.dataStreams().get(dsName);
            assertThat(resolvedIndices.getLocal(), hasItem(dsName));
            for (Index i : dataStream.getIndices()) {
                assertThat(resolvedIndices.getLocal(), hasItem(i.getName()));
            }
        }
    }

    public void testDataStreamsAreNotVisibleWhenNotIncludedByRequestWithoutWildcard() {
        final User user = new User("data-stream-tester2", "data_stream_test2");
        String dataStreamName = "logs-foobar";
        GetAliasesRequest request = new GetAliasesRequest(dataStreamName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams and their backing indices should _not_ be in the authorized list since the backing indices
        // do not match the requested name
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        assertThat(authorizedIndices, hasItem(dataStreamName));
        DataStream dataStream = metadata.dataStreams().get(dataStreamName);
        assertThat(authorizedIndices, hasItem(dataStreamName));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }

        // neither data streams nor their backing indices will be in the resolved list since the backing indices do not match the
        // requested name(s)
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            GetAliasesAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), hasItem(dataStreamName));
        for (Index i : dataStream.getIndices()) {
            assertThat(resolvedIndices.getLocal(), hasItem(i.getName()));
        }
    }

    public void testDataStreamsAreVisibleWhenIncludedByRequestWithWildcard() {
        final User user = new User("data-stream-tester3", "data_stream_test3");
        SearchRequest request = new SearchRequest("logs*");
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams and their backing indices should be in the authorized list
        List<String> expectedDataStreams = org.elasticsearch.core.List.of("logs-foo", "logs-foobar");
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME, request);
        for (String dsName : expectedDataStreams) {
            DataStream dataStream = metadata.dataStreams().get(dsName);
            assertThat(authorizedIndices, hasItem(dsName));
            for (Index i : dataStream.getIndices()) {
                assertThat(authorizedIndices, hasItem(i.getName()));
            }
        }

        // data streams without their backing indices will be in the resolved list since the backing indices do not match the requested
        // pattern
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            SearchAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), hasItem("logs-foo"));
        assertThat(resolvedIndices.getLocal(), hasItem("logs-foobar"));
        assertThat(resolvedIndices.getLocal(), hasItem("logs-00001"));
        assertThat(resolvedIndices.getLocal(), hasItem("logs-00002"));
        assertThat(resolvedIndices.getLocal(), hasItem("logs-00003"));
        assertThat(resolvedIndices.getLocal(), hasItem("logs-alias"));
        for (String dsName : expectedDataStreams) {
            DataStream dataStream = metadata.dataStreams().get(dsName);
            assertNotNull(dataStream);
            for (Index i : dataStream.getIndices()) {
                assertThat(resolvedIndices.getLocal(), not(hasItem(i.getName())));
            }
        }
    }

    public void testDataStreamsAreVisibleWhenIncludedByRequestWithoutWildcard() {
        final User user = new User("data-stream-tester3", "data_stream_test3");
        String dataStreamName = "logs-foobar";
        DataStream dataStream = metadata.dataStreams().get(dataStreamName);
        SearchRequest request = new SearchRequest(dataStreamName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME, request);
        // data streams and their backing indices should be in the authorized list
        assertThat(authorizedIndices, hasItem(dataStreamName));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }

        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            SearchAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        // data streams without their backing indices will be in the resolved list since the backing indices do not match the requested
        // name
        assertThat(resolvedIndices.getLocal(), hasItem(dataStreamName));
        for (Index i : dataStream.getIndices()) {
            assertThat(resolvedIndices.getLocal(), not(hasItem(i.getName())));
        }
    }

    public void testBackingIndicesAreVisibleWhenIncludedByRequestWithWildcard() {
        final User user = new User("data-stream-tester3", "data_stream_test3");
        SearchRequest request = new SearchRequest(".ds-logs*");
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams and their backing indices should be included in the authorized list
        List<String> expectedDataStreams = org.elasticsearch.core.List.of("logs-foo", "logs-foobar");
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME, request);
        for (String dsName : expectedDataStreams) {
            DataStream dataStream = metadata.dataStreams().get(dsName);
            assertThat(authorizedIndices, hasItem(dsName));
            for (Index i : dataStream.getIndices()) {
                assertThat(authorizedIndices, hasItem(i.getName()));
            }
        }

        // data streams should _not_ be included in the resolved list because they do not match the pattern but their backing indices
        // should be in the resolved list because they match the pattern and are authorized via extension from their parent data stream
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            SearchAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        for (String dsName : expectedDataStreams) {
            DataStream dataStream = metadata.dataStreams().get(dsName);
            assertThat(resolvedIndices.getLocal(), not(hasItem(dsName)));
            for (Index i : dataStream.getIndices()) {
                assertThat(resolvedIndices.getLocal(), hasItem(i.getName()));
            }
        }
    }

    public void testBackingIndicesAreNotVisibleWhenNotIncludedByRequestWithoutWildcard() {
        final User user = new User("data-stream-tester2", "data_stream_test2");
        String dataStreamName = "logs-foobar";
        GetAliasesRequest request = new GetAliasesRequest(dataStreamName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams and their backing indices should _not_ be in the authorized list since the backing indices
        // did not match the requested pattern and the request does not support data streams
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        assertThat(authorizedIndices, hasItem(dataStreamName));
        DataStream dataStream = metadata.dataStreams().get(dataStreamName);
        assertThat(authorizedIndices, hasItem(dataStreamName));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }

        // neither data streams nor their backing indices will be in the resolved list since the request does not support data streams
        // and the backing indices do not match the requested name
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            GetAliasesAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), hasItem(dataStreamName));
        for (Index i : dataStream.getIndices()) {
            assertThat(resolvedIndices.getLocal(), hasItem(i.getName()));
        }
    }

    public void testDataStreamNotAuthorizedWhenBackingIndicesAreAuthorizedViaWildcardAndRequestThatIncludesDataStreams() {
        final User user = new User("data-stream-tester2", "backing_index_test_wildcards");
        String indexName = ".ds-logs-foobar-*";
        SearchRequest request = new SearchRequest(indexName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams should _not_ be in the authorized list but their backing indices that matched both the requested pattern
        // and the authorized pattern should be in the list
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        assertThat(authorizedIndices, not(hasItem("logs-foobar")));
        DataStream dataStream = metadata.dataStreams().get("logs-foobar");
        assertThat(authorizedIndices, not(hasItem(indexName)));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }

        // only the backing indices will be in the resolved list since the request does not support data streams
        // but the backing indices match the requested pattern
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            SearchAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), not(hasItem(dataStream.getName())));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }
    }

    public void testDataStreamNotAuthorizedWhenBackingIndicesAreAuthorizedViaNameAndRequestThatIncludesDataStreams() {
        final User user = new User("data-stream-tester2", "backing_index_test_name");
        String indexName = ".ds-logs-foobar-*";
        SearchRequest request = new SearchRequest(indexName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams should _not_ be in the authorized list but a single backing index that matched the requested pattern
        // and the authorized name should be in the list
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        assertThat(authorizedIndices, not(hasItem("logs-foobar")));
        assertThat(authorizedIndices, contains(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));

        // only the single backing index will be in the resolved list since the request does not support data streams
        // but one of the backing indices matched the requested pattern
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            SearchAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), not(hasItem("logs-foobar")));
        assertThat(resolvedIndices.getLocal(), contains(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
    }

    public void testDataStreamNotAuthorizedWhenBackingIndicesAreAuthorizedViaWildcardAndRequestThatExcludesDataStreams() {
        final User user = new User("data-stream-tester2", "backing_index_test_wildcards");
        String indexName = ".ds-logs-foobar-*";
        GetAliasesRequest request = new GetAliasesRequest(indexName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams should _not_ be in the authorized list but their backing indices that matched both the requested pattern
        // and the authorized pattern should be in the list
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        assertThat(authorizedIndices, not(hasItem("logs-foobar")));
        DataStream dataStream = metadata.dataStreams().get("logs-foobar");
        assertThat(authorizedIndices, not(hasItem(indexName)));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }

        // only the backing indices will be in the resolved list since the request does not support data streams
        // but the backing indices match the requested pattern
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            GetAliasesAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), not(hasItem(dataStream.getName())));
        for (Index i : dataStream.getIndices()) {
            assertThat(authorizedIndices, hasItem(i.getName()));
        }
    }

    public void testDataStreamNotAuthorizedWhenBackingIndicesAreAuthorizedViaNameAndRequestThatExcludesDataStreams() {
        final User user = new User("data-stream-tester2", "backing_index_test_name");
        String indexName = ".ds-logs-foobar-*";
        GetAliasesRequest request = new GetAliasesRequest(indexName);
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.includeDataStreams(), is(true));

        // data streams should _not_ be in the authorized list but a single backing index that matched the requested pattern
        // and the authorized name should be in the list
        final Set<String> authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME, request);
        assertThat(authorizedIndices, not(hasItem("logs-foobar")));
        assertThat(authorizedIndices, contains(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));

        // only the single backing index will be in the resolved list since the request does not support data streams
        // but one of the backing indices matched the requested pattern
        ResolvedIndices resolvedIndices = defaultIndicesResolver.resolveIndicesAndAliases(
            GetAliasesAction.NAME,
            request,
            metadata,
            authorizedIndices
        );
        assertThat(resolvedIndices.getLocal(), not(hasItem("logs-foobar")));
        assertThat(resolvedIndices.getLocal(), contains(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
    }

    private Set<String> buildAuthorizedIndices(User user, String action) {
        return buildAuthorizedIndices(user, action, TransportRequest.Empty.INSTANCE);
    }

    private Set<String> buildAuthorizedIndices(User user, String action, TransportRequest request) {
        PlainActionFuture<Role> rolesListener = new PlainActionFuture<>();
        final Authentication authentication =
            new Authentication(user, new RealmRef("test", "indices-aliases-resolver-tests", "node"), null);
        rolesStore.getRoles(user, authentication, rolesListener);
        return RBACEngine.resolveAuthorizedIndicesFromRole(rolesListener.actionGet(), getRequestInfo(request, action),
            metadata.getIndicesLookup());
    }

    public static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index).settings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    private ResolvedIndices resolveIndices(TransportRequest request, Set<String> authorizedIndices) {
        return resolveIndices("indices:/" + randomAlphaOfLength(8), request, authorizedIndices);
    }

    private ResolvedIndices resolveIndices(String action, TransportRequest request, Set<String> authorizedIndices) {
        return defaultIndicesResolver.resolve(action, request, this.metadata, authorizedIndices);
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
