/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiKeyServiceRoleSubsetTests extends AbstractBuilderTestCase {
    private AuthorizationService mockAuthzService = mock(AuthorizationService.class);
    private ScriptService mockScriptService = mock(ScriptService.class);
    private ClusterService mockClusterService = mock(ClusterService.class);
    private SecurityIndexManager mockSecurityIndexManager = mock(SecurityIndexManager.class);
    private Clock mockCock = mock(Clock.class);
    private Client mockClient = mock(Client.class);
    private ApiKeyService apiKeyService;

    private User userForNotASubsetRole;
    private User userWithSuperUserRole;
    private User userWithRoleForDLS;

    @Before
    public void setup() {
        MustacheScriptEngine mse = new MustacheScriptEngine();
        Map<String, ScriptEngine> engines = Collections.singletonMap(mse.getType(), mse);
        Map<String, ScriptContext<?>> contexts = Collections.singletonMap(TemplateScript.CONTEXT.name, TemplateScript.CONTEXT);
        mockScriptService = new ScriptService(Settings.EMPTY, engines, contexts);
        apiKeyService = new ApiKeyService(Settings.EMPTY, mockCock, mockClient, mockSecurityIndexManager, mockClusterService,
                mockAuthzService, mockScriptService, xContentRegistry());
        userForNotASubsetRole = new User("user_not_a_subset", "not-a-subset-role");
        userWithSuperUserRole = new User("superman", "superuser");
        userWithRoleForDLS = new User("user_with_2_roles_with_dls", "base-role-1", "base-role-2");
        mockRoleDescriptors();
        mockRolesForUser();
        mockRolesForRoleDescriptors();
    }

    public void testWhenRoleDescriptorsAreNotASubsetThrowsException() throws IOException {
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(userForNotASubsetRole);

        final List<RoleDescriptor> requestRoleDescriptors = new ArrayList<>();
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-1", new String[] { "index-1-1-1-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD1\" } }"));
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-2", new String[] { "index-1-1-2-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD2\" } }"));
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> {
            apiKeyService
            .checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication);
        });
        assertThat(ese.getMessage(), equalTo("role descriptors from the request are not subset of the authenticated user"));
    }

    public void testWhenRoleDescriptorsAreSubsetSoNoModificationsAreRequired() throws IOException {
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(userWithSuperUserRole);

        final List<RoleDescriptor> requestRoleDescriptors = new ArrayList<>();
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-1", new String[] { "index-1-1-1-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD1\" } }"));
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-2", new String[] { "index-1-1-2-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD2\" } }"));
        List<RoleDescriptor> newChildDescriptors = apiKeyService
                .checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication);
        assertThat(newChildDescriptors, equalTo(requestRoleDescriptors));
    }

    public void testCheckRoleSubsetIsMaybeAndRoleDescriptorsAreModified() throws IOException {
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(userWithRoleForDLS);

        final List<RoleDescriptor> requestRoleDescriptors = new ArrayList<>();
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-1", new String[] { "index-1-1-1-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD1\" } }"));
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-2", new String[] { "index-1-1-2-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD2\" } }"));
        List<RoleDescriptor> newChildDescriptors = apiKeyService
                .checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication);

        assertThat(newChildDescriptors.size(), equalTo(2));

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        CompositeRolesStore.buildRoleFromDescriptors(newChildDescriptors, new FieldPermissionsCache(Settings.EMPTY), null, future);
        final Role finalRole = future.actionGet();
        final IndexMetaData.Builder imbBuilder = IndexMetaData
                .builder("index-1-1-1-1").settings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .putAlias(AliasMetaData.builder("_1111"));
        final MetaData md = MetaData.builder().put(imbBuilder).build();
        final IndicesAccessControl iac = finalRole.authorize(SearchAction.NAME, Sets.newHashSet("index-1-1-1-1"), md,
                new FieldPermissionsCache(Settings.EMPTY));

        assertThat(iac.isGranted(), equalTo(true));
        assertThat(iac.getIndexPermissions("index-1-1-1-1").getQueries().size(), is(1));
        iac.getIndexPermissions("index-1-1-1-1").getQueries().stream().forEach(q -> {
            try {
                QueryBuilder queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(createParser(XContentType.JSON.xContent(), q));
                assertThat(queryBuilder, instanceOf(BoolQueryBuilder.class));
                BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;

                // Verify should (from child)
                assertThat(boolQueryBuilder.should().size(), is(1));
                assertThat(boolQueryBuilder.minimumShouldMatch(), is("1"));
                {
                    QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                    assertThat(shouldQueryBuilder, instanceOf(MatchQueryBuilder.class));
                    MatchQueryBuilder shouldMatchQB = (MatchQueryBuilder) shouldQueryBuilder;
                    String fieldName = shouldMatchQB.fieldName();
                    assertThat(fieldName, equalTo("category"));
                    String value = (String) shouldMatchQB.value();
                    assertThat(value, equalTo("RD1"));
                }

                // Verify filter (from base)
                assertThat(boolQueryBuilder.filter().size(), is(1));
                {
                    QueryBuilder filterBoolQueryBuilder = boolQueryBuilder.filter().get(0);
                    assertThat(filterBoolQueryBuilder, instanceOf(BoolQueryBuilder.class));
                    BoolQueryBuilder filter = (BoolQueryBuilder) filterBoolQueryBuilder;

                    assertThat(filter.should().size(), is(2));
                    assertThat(filter.minimumShouldMatch(), is("1"));
                    Set<String> valuesExpected = Sets.newHashSet("BRD1", "BRD2");
                    Set<String> actualValues = new HashSet<>();
                    for (QueryBuilder qb : filter.should()) {
                        assertThat(qb, instanceOf(MatchQueryBuilder.class));
                        MatchQueryBuilder matchQB = (MatchQueryBuilder) qb;
                        String fieldName = matchQB.fieldName();
                        assertThat(fieldName, equalTo("category"));
                        String value = (String) matchQB.value();
                        actualValues.add(value);
                    }
                    assertThat(actualValues, equalTo(valuesExpected));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testCheckRoleSubsetIsMaybeAndRoleDescriptorsAreModifiedAndAlsoEvaluatesTemplate() throws IOException {
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(userWithRoleForDLS);
        final List<RoleDescriptor> requestRoleDescriptors = new ArrayList<>();
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-1", new String[] { "index-1-1-1-*" }, new String[] { "READ" },
                "{ \"template\": { \"source\" : { \"term\": { \"category\" : \"{{_user.username}}\" } } } }"));
        requestRoleDescriptors.add(buildRoleDescriptor("child-rd-2", new String[] { "index-1-1-2-*" }, new String[] { "READ" },
                "{ \"match\": { \"category\": \"RD2\" } }"));
        List<RoleDescriptor> newChildDescriptors = apiKeyService
                .checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication);

        assertThat(newChildDescriptors.size(), equalTo(2));

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        CompositeRolesStore.buildRoleFromDescriptors(newChildDescriptors, new FieldPermissionsCache(Settings.EMPTY), null, future);
        final Role finalRole = future.actionGet();
        final IndexMetaData.Builder imbBuilder = IndexMetaData
                .builder("index-1-1-1-1").settings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .putAlias(AliasMetaData.builder("_1111"));
        final MetaData md = MetaData.builder().put(imbBuilder).build();
        final IndicesAccessControl iac = finalRole.authorize(SearchAction.NAME, Sets.newHashSet("index-1-1-1-1"), md,
                new FieldPermissionsCache(Settings.EMPTY));

        assertThat(iac.isGranted(), equalTo(true));
        assertThat(iac.getIndexPermissions("index-1-1-1-1").getQueries().size(), is(1));
        iac.getIndexPermissions("index-1-1-1-1").getQueries().stream().forEach(q -> {
            try {
                QueryBuilder queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(createParser(XContentType.JSON.xContent(), q));
                assertThat(queryBuilder, instanceOf(BoolQueryBuilder.class));
                BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;

                // Verify should (from child)
                assertThat(boolQueryBuilder.should().size(), is(1));
                assertThat(boolQueryBuilder.minimumShouldMatch(), is("1"));
                {
                    QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                    assertThat(shouldQueryBuilder, instanceOf(TermQueryBuilder.class));
                    TermQueryBuilder termQB = (TermQueryBuilder) shouldQueryBuilder;
                    String fieldName = termQB.fieldName();
                    assertThat(fieldName, equalTo("category"));
                    String value = (String) termQB.value();
                    assertThat(value, equalTo("user_with_2_roles_with_dls"));
                }

                // Verify filter (from base)
                assertThat(boolQueryBuilder.filter().size(), is(1));
                {
                    QueryBuilder filterBoolQueryBuilder = boolQueryBuilder.filter().get(0);
                    assertThat(filterBoolQueryBuilder, instanceOf(BoolQueryBuilder.class));
                    BoolQueryBuilder filter = (BoolQueryBuilder) filterBoolQueryBuilder;

                    assertThat(filter.should().size(), is(2));
                    assertThat(filter.minimumShouldMatch(), is("1"));
                    Set<String> valuesExpected = Sets.newHashSet("BRD1", "BRD2");
                    Set<String> actualValues = new HashSet<>();
                    for (QueryBuilder qb : filter.should()) {
                        assertThat(qb, instanceOf(MatchQueryBuilder.class));
                        MatchQueryBuilder matchQB = (MatchQueryBuilder) qb;
                        String fieldName = matchQB.fieldName();
                        assertThat(fieldName, equalTo("category"));
                        String value = (String) matchQB.value();
                        actualValues.add(value);
                    }
                    assertThat(actualValues, equalTo(valuesExpected));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void mockRolesForRoleDescriptors() {
        doAnswer((Answer) invocation -> {
            final List<RoleDescriptor> roleDescriptors = (List<RoleDescriptor>) invocation.getArguments()[0];
            final ActionListener<Role> roleActionListener = (ActionListener<Role>) invocation.getArguments()[1];
            CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, new FieldPermissionsCache(Settings.EMPTY), null,
                    roleActionListener);

            return null;
        }).when(mockAuthzService).roles(any(List.class), any(ActionListener.class));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void mockRolesForUser() {
        doAnswer((Answer) invocation -> {
            final User user = (User) invocation.getArguments()[0];
            final ActionListener<Role> roleActionListener = (ActionListener<Role>) invocation.getArguments()[1];

            switch (user.principal()) {
            case "user_not_a_subset": {
                final PlainActionFuture<Set<RoleDescriptor>> roleDescriptorsActionListener = new PlainActionFuture<>();
                mockAuthzService.roleDescriptors(user, roleDescriptorsActionListener);
                Set<RoleDescriptor> roleDescriptors = roleDescriptorsActionListener.actionGet();
                CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, new FieldPermissionsCache(Settings.EMPTY), null,
                        roleActionListener);
                break;
            }
            case "superman": {
                roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
                break;
            }
            case "user_with_2_roles_with_dls": {
                final PlainActionFuture<Set<RoleDescriptor>> roleDescriptorsActionListener = new PlainActionFuture<>();
                mockAuthzService.roleDescriptors(user, roleDescriptorsActionListener);
                Set<RoleDescriptor> roleDescriptors = roleDescriptorsActionListener.actionGet();
                CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, new FieldPermissionsCache(Settings.EMPTY), null,
                        roleActionListener);
                break;
            }
            }

            return null;
        }).when(mockAuthzService).roles(any(User.class), any(ActionListener.class));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void mockRoleDescriptors() {
        doAnswer((Answer) invocation -> {
            final User user = (User) invocation.getArguments()[0];
            final ActionListener<Set<RoleDescriptor>> roleDescriptorsActionListener = (ActionListener<Set<RoleDescriptor>>) invocation
                    .getArguments()[1];

            switch (user.principal()) {
            case "user_not_a_subset": {
                final Set<RoleDescriptor> roleDescriptorsWithDls = new HashSet<>();
                roleDescriptorsWithDls.add(buildRoleDescriptor("not-a-subset-role", new String[] { "index-not-subset-*" },
                        new String[] { "WRITE" }, "{ \"match\": { \"category\": \"UNKNOWN\" } }"));
                roleDescriptorsActionListener.onResponse(roleDescriptorsWithDls);
                break;
            }
            case "superman": {
                roleDescriptorsActionListener.onResponse(Collections.singleton(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
                break;
            }
            case "user_with_2_roles_with_dls": {
                final Set<RoleDescriptor> roleDescriptorsWithDls = new HashSet<>();
                roleDescriptorsWithDls.add(buildRoleDescriptor("base-rd-1", new String[] { "index-1-1-*" }, new String[] { "READ" },
                        "{ \"match\": { \"category\": \"BRD1\" } }"));
                roleDescriptorsWithDls.add(buildRoleDescriptor("base-rd-2", new String[] { "index-1-1-1-*", "index-1-1-2-*" },
                        new String[] { "READ" }, "{ \"match\": { \"category\": \"BRD2\" } }"));
                roleDescriptorsActionListener.onResponse(roleDescriptorsWithDls);
                break;
            }
            }

            return null;
        }).when(mockAuthzService).roleDescriptors(any(User.class), any(ActionListener.class));
    }

    private static RoleDescriptor buildRoleDescriptor(String name, String[] indices, String[] privileges, String query) {
        IndicesPrivileges roleDescriptorIndicesPrivileges = IndicesPrivileges.builder()
                .indices(indices)
                .privileges(privileges)
                .query(query)
                .build();
        return new RoleDescriptor(name, null, new IndicesPrivileges[] { roleDescriptorIndicesPrivileges }, null);
    }
}
