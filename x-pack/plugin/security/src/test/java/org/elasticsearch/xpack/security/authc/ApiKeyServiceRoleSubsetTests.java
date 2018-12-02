/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult.Result;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyServiceRoleSubsetTests.ExpectedResult.ExpectedRDQueryDetails;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiKeyServiceRoleSubsetTests extends AbstractBuilderTestCase {
    private static final Logger logger = LogManager.getLogger(ApiKeyServiceRoleSubsetTests.class);

    private CompositeRolesStore compositeRolesStore = mock(CompositeRolesStore.class);
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
                compositeRolesStore, mockScriptService, xContentRegistry());
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
        final PlainActionFuture<List<RoleDescriptor>> roleDescriptorsFuture = new PlainActionFuture<>();
        apiKeyService.checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication,
                roleDescriptorsFuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> roleDescriptorsFuture.actionGet());
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
        final PlainActionFuture<List<RoleDescriptor>> newChildDescriptorsFuture = new PlainActionFuture<>();
        apiKeyService.checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication,
                newChildDescriptorsFuture);
        List<RoleDescriptor> newChildDescriptors = newChildDescriptorsFuture.actionGet();
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
        final PlainActionFuture<List<RoleDescriptor>> newChildDescriptorsFuture = new PlainActionFuture<>();
        apiKeyService.checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication,
                newChildDescriptorsFuture);
        final List<RoleDescriptor> newChildDescriptors = newChildDescriptorsFuture.actionGet();
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
        final PlainActionFuture<List<RoleDescriptor>> newChildDescriptorsFuture = new PlainActionFuture<>();
        apiKeyService.checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(requestRoleDescriptors, authentication,
                newChildDescriptorsFuture);
        List<RoleDescriptor> newChildDescriptors = newChildDescriptorsFuture.actionGet();

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

    static class ExpectedResult {
        static class ExpectedRDQueryDetails {
            boolean hasQuery;
            String[] categoryValuesExpectedInQueryFromExistingRDs;
            String[] categoryValuesExpectedInQueryFromSubsetRDs;
            @Override
            public String toString() {
                return "ExpectedRDQueryDetails [hasQuery=" + hasQuery + ", categoryValuesExpectedInQueryFromExistingRDs="
                        + Arrays.toString(categoryValuesExpectedInQueryFromExistingRDs) + ", categoryValuesExpectedInQueryFromSubsetRDs="
                        + Arrays.toString(categoryValuesExpectedInQueryFromSubsetRDs) + "]";
            }
        }
        SubsetResult.Result result;
        @Override
        public String toString() {
            return "ExpectedResult [result=" + result + ", expectedQueryDetailsForRD=" + expectedQueryDetailsForRD + "]";
        }
        Map<String, ExpectedRDQueryDetails> expectedQueryDetailsForRD = new HashMap<>();
        static ExpectedResult of(SubsetResult.Result result) {
            ExpectedResult res = new ExpectedResult();
            res.result = result;
            return res;
        }
        ExpectedResult add(String rdName, boolean hasQuery, String[] valuesFromExisting, String[] valuesFromSubset) {
            ExpectedRDQueryDetails inner = new ExpectedRDQueryDetails();
            inner.hasQuery = hasQuery;
            inner.categoryValuesExpectedInQueryFromExistingRDs = valuesFromExisting;
            inner.categoryValuesExpectedInQueryFromSubsetRDs = valuesFromSubset;
            this.expectedQueryDetailsForRD.put(rdName, inner);
            return this;
        }
    }

    static List<List<RoleDescriptor>> existingRDTestData = new ArrayList<>();
    static List<List<RoleDescriptor>> subsetRDTestData = new ArrayList<>();
    static {
        List<RoleDescriptor> op1 = new ArrayList<>();
        op1.add(buildRoleDescriptor("erd1", new String[] { "index1*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"ERD_Q1\" } }"));
        List<RoleDescriptor> op2 = new ArrayList<>();
        op2.add(buildRoleDescriptor("erd1", new String[] { "index1*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"ERD_Q1\" } }"));
        op2.add(buildRoleDescriptor("erd2", new String[] { "index11*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"ERD_Q11\" } }"));
        op2.add(buildRoleDescriptor("erd3", new String[] { "index2*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"ERD_Q2\" } }"));
        List<RoleDescriptor> op3 = new ArrayList<>();
        op3.add(buildRoleDescriptor("erd1", new String[] { "index1*" }, new String[] { "all" }, null));
        op3.add(buildRoleDescriptor("erd2", new String[] { "index11*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"ERD_Q11\" } }"));
        op3.add(buildRoleDescriptor("erd3", new String[] { "index2*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"ERD_Q2\" } }"));
        existingRDTestData.add(op1);
        existingRDTestData.add(op2);
        existingRDTestData.add(op3);

        List<RoleDescriptor> sop1 = new ArrayList<>();
        sop1.add(buildRoleDescriptor("srd1", new String[] { "index11" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"SRD_Q1\" } }"));
        List<RoleDescriptor> sop2 = new ArrayList<>();
        sop2.add(buildRoleDescriptor("srd1", new String[] { "index1*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"SRD_Q1\" } }"));
        sop2.add(buildRoleDescriptor("srd2", new String[] { "index11*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"SRD_Q11\" } }"));
        List<RoleDescriptor> sop3 = new ArrayList<>();
        sop3.add(buildRoleDescriptor("srd1", new String[] { "index1*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"SRD_Q1\" } }"));
        sop3.add(buildRoleDescriptor("srd2", new String[] { "index2*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"SRD_Q2\" } }"));
        List<RoleDescriptor> sop4 = new ArrayList<>();
        sop4.add(buildRoleDescriptor("srd1", new String[] { "index*" }, new String[] { "all" },
                "{ \"match\": { \"category\": \"SRD_Q1\" } }"));
        subsetRDTestData.add(sop1);
        subsetRDTestData.add(sop2);
        subsetRDTestData.add(sop3);
        subsetRDTestData.add(sop4);
    }

    static ExpectedResult[][] expectedResultsForCombination = new ExpectedResult[][] {
            {
                ExpectedResult.of(SubsetResult.Result.MAYBE).add("srd1", true, new String[] { "ERD_Q1" }, new String[] { "SRD_Q1" }),
                ExpectedResult.of(SubsetResult.Result.MAYBE).add("srd1", true, new String[] { "ERD_Q1", "ERD_Q11" },
                            new String[] { "SRD_Q1" }),
                ExpectedResult.of(SubsetResult.Result.YES).add("srd1", true, new String[] {}, new String[] { "SRD_Q1" })
            },
            {
                ExpectedResult.of(SubsetResult.Result.MAYBE).add("srd1", true, new String[] { "ERD_Q1" }, new String[] { "SRD_Q1" })
                    .add("srd2", true, new String[] { "ERD_Q1" }, new String[] { "SRD_Q11" }),
                ExpectedResult.of(SubsetResult.Result.MAYBE).add("srd1", true, new String[] { "ERD_Q1" }, new String[] { "SRD_Q1" })
                            .add("srd2", true, new String[] { "ERD_Q1", "ERD_Q11" }, new String[] { "SRD_Q11" }),
                ExpectedResult.of(SubsetResult.Result.YES).add("srd1", true, new String[] {}, new String[] { "SRD_Q1" }).add("srd2",
                            true, new String[] {}, new String[] { "SRD_Q11" })
            },
            {
                ExpectedResult.of(SubsetResult.Result.NO),
                ExpectedResult.of(SubsetResult.Result.MAYBE).add("srd1", true, new String[] { "ERD_Q1" }, new String[] { "SRD_Q1" })
                            .add("srd2", true, new String[] { "ERD_Q2" }, new String[] { "SRD_Q2" }),
                ExpectedResult.of(SubsetResult.Result.MAYBE).add("srd1", true, new String[] {}, new String[] { "SRD_Q1" }).add("srd2",
                            true, new String[] { "ERD_Q2" }, new String[] { "SRD_Q2" })
            },
            {
                ExpectedResult.of(SubsetResult.Result.NO),
                ExpectedResult.of(SubsetResult.Result.NO),
                ExpectedResult.of(SubsetResult.Result.NO)
            }
       };

    public void testRoleDescriptorModifyDifferentCombinations() throws IOException {
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 3; col++) {
                final List<RoleDescriptor> subsetRoleDescriptors = subsetRDTestData.get(row);
                final List<RoleDescriptor> existingRoleDescriptors = existingRDTestData.get(col);
                logger.info("Subset Role Descriptors:\n" + subsetRoleDescriptors);
                logger.info("Existing Role Descriptors:\n" + existingRoleDescriptors);
                logger.info(expectedResultsForCombination[row][col]);
                logger.info("----------------------------------------------------------------------");

                final PlainActionFuture<Role> subsetRoleListener = new PlainActionFuture<>();
                CompositeRolesStore.buildRoleFromDescriptors(subsetRoleDescriptors, new FieldPermissionsCache(Settings.EMPTY), null,
                        subsetRoleListener);
                final Role subsetRole = subsetRoleListener.actionGet();
                final PlainActionFuture<Role> existingRoleListener = new PlainActionFuture<>();
                CompositeRolesStore.buildRoleFromDescriptors(existingRoleDescriptors, new FieldPermissionsCache(Settings.EMPTY), null,
                        existingRoleListener);
                final Role existingRole = existingRoleListener.actionGet();

                final SubsetResult result = subsetRole.isSubsetOf(existingRole);
                assertThat("unexpected result for row = " + row + ", col = " + col, result.result(),
                        equalTo(expectedResultsForCombination[row][col].result));
                if (result.result() == Result.NO) {
                    // Nothing to verify
                    continue;
                }
                final List<RoleDescriptor> newChildDescriptors = apiKeyService.modifyRoleDescriptorsToMakeItASubset(subsetRoleDescriptors,
                        existingRoleDescriptors, result, null);

                assertThat(newChildDescriptors.size(), equalTo(subsetRoleDescriptors.size()));

                for (RoleDescriptor newModifiedRD : newChildDescriptors) {
                    ExpectedResult expectedResult = expectedResultsForCombination[row][col];
                    ExpectedRDQueryDetails inner = expectedResult.expectedQueryDetailsForRD.get(newModifiedRD.getName());

                    for (IndicesPrivileges ip : newModifiedRD.getIndicesPrivileges()) {
                        if (inner != null && inner.hasQuery) {
                            try {
                                QueryBuilder queryBuilder = AbstractQueryBuilder
                                        .parseInnerQueryBuilder(createParser(XContentType.JSON.xContent(), ip.getQuery()));

                                if (inner.categoryValuesExpectedInQueryFromExistingRDs == null
                                        || inner.categoryValuesExpectedInQueryFromExistingRDs.length == 0) {
                                    assertThat(queryBuilder, instanceOf(MatchQueryBuilder.class));
                                    MatchQueryBuilder matchQB = (MatchQueryBuilder) queryBuilder;
                                    Set<String> valuesExpected = Sets.newHashSet(inner.categoryValuesExpectedInQueryFromSubsetRDs);
                                    Set<String> actualValues = new HashSet<>();
                                    String fieldName = matchQB.fieldName();
                                    assertThat(fieldName, equalTo("category"));
                                    String value = (String) matchQB.value();
                                    actualValues.add(value);
                                    assertThat(actualValues, equalTo(valuesExpected));
                                } else {
                                    assertThat(queryBuilder, instanceOf(BoolQueryBuilder.class));
                                    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;

                                    // Verify should (from subset rd)
                                    assertThat(boolQueryBuilder.should().size(), is(1));
                                    assertThat(boolQueryBuilder.minimumShouldMatch(), is("1"));
                                    {
                                        QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                                        assertThat(shouldQueryBuilder, instanceOf(MatchQueryBuilder.class));
                                        MatchQueryBuilder matchQB = (MatchQueryBuilder) shouldQueryBuilder;
                                        String fieldName = matchQB.fieldName();
                                        assertThat(fieldName, equalTo("category"));
                                        String value = (String) matchQB.value();
                                        assertThat(value, equalTo(inner.categoryValuesExpectedInQueryFromSubsetRDs[0]));
                                    }

                                    // Verify filter (from existing rd)
                                    assertThat(boolQueryBuilder.filter().size(), is(1));
                                    {
                                        QueryBuilder filterBoolQueryBuilder = boolQueryBuilder.filter().get(0);
                                        assertThat(filterBoolQueryBuilder, instanceOf(BoolQueryBuilder.class));
                                        BoolQueryBuilder filter = (BoolQueryBuilder) filterBoolQueryBuilder;

                                        assertThat(filter.should().size(), is(inner.categoryValuesExpectedInQueryFromExistingRDs.length));
                                        assertThat(filter.minimumShouldMatch(), is("1"));
                                        Set<String> valuesExpected = Sets.newHashSet(inner.categoryValuesExpectedInQueryFromExistingRDs);
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
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            assertThat(ip.getQuery(), nullValue());
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void mockRolesForRoleDescriptors() {
        doAnswer((Answer) invocation -> {
            final List<RoleDescriptor> roleDescriptors = (List<RoleDescriptor>) invocation.getArguments()[0];
            final FieldPermissionsCache fieldPermissionsCache = (FieldPermissionsCache) invocation.getArguments()[1];
            final ActionListener<Role> roleActionListener = (ActionListener<Role>) invocation.getArguments()[2];
            CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, null,
                    roleActionListener);

            return null;
        }).when(compositeRolesStore).roles(any(List.class), any(FieldPermissionsCache.class), any(ActionListener.class));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void mockRolesForUser() {
        doAnswer((Answer) invocation -> {
            final User user = (User) invocation.getArguments()[0];
            final FieldPermissionsCache fieldPermissionsCache = (FieldPermissionsCache) invocation.getArguments()[1];
            final ActionListener<Role> roleActionListener = (ActionListener<Role>) invocation.getArguments()[2];

            switch (user.principal()) {
            case "user_not_a_subset": {
                final PlainActionFuture<Set<RoleDescriptor>> roleDescriptorsActionListener = new PlainActionFuture<>();
                compositeRolesStore.getRoleDescriptors(user, roleDescriptorsActionListener);
                Set<RoleDescriptor> roleDescriptors = roleDescriptorsActionListener.actionGet();
                CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, null,
                        roleActionListener);
                break;
            }
            case "superman": {
                roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
                break;
            }
            case "user_with_2_roles_with_dls": {
                final PlainActionFuture<Set<RoleDescriptor>> roleDescriptorsActionListener = new PlainActionFuture<>();
                compositeRolesStore.getRoleDescriptors(user, roleDescriptorsActionListener);
                Set<RoleDescriptor> roleDescriptors = roleDescriptorsActionListener.actionGet();
                CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, null,
                        roleActionListener);
                break;
            }
            }

            return null;
        }).when(compositeRolesStore).roles(any(User.class), any(FieldPermissionsCache.class), any(ActionListener.class));
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
        }).when(compositeRolesStore).getRoleDescriptors(any(User.class), any(ActionListener.class));
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
