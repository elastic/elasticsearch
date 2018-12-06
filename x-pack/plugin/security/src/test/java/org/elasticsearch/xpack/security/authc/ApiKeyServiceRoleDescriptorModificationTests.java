/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult.Result;
import org.elasticsearch.xpack.security.authc.ApiKeyServiceRoleDescriptorModificationTests.ExpectedResult.ExpectedRDQueryDetails;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests for different combination of role descriptors subset check and
 * modifications to make it a subset.
 */
public class ApiKeyServiceRoleDescriptorModificationTests extends AbstractBuilderTestCase {
    private static final Logger logger = LogManager.getLogger(ApiKeyServiceRoleDescriptorModificationTests.class);

    private static List<List<RoleDescriptor>> EXISTING_RD_TEST_DATA = initializeExistingRoleDescriptorsTestData();
    private static List<List<RoleDescriptor>> SUBSET_RD_TEST_DATA = initializeSubsetRoleDescriptorsTestData();

    private CompositeRolesStore compositeRolesStore = mock(CompositeRolesStore.class);
    private ScriptService mockScriptService = mock(ScriptService.class);
    private ClusterService mockClusterService = mock(ClusterService.class);
    private SecurityIndexManager mockSecurityIndexManager = mock(SecurityIndexManager.class);
    private Clock mockCock = mock(Clock.class);
    private Client mockClient = mock(Client.class);
    private ApiKeyService apiKeyService;

    @Before
    public void setup() {
        MustacheScriptEngine mse = new MustacheScriptEngine();
        Map<String, ScriptEngine> engines = Collections.singletonMap(mse.getType(), mse);
        Map<String, ScriptContext<?>> contexts = Collections.singletonMap(TemplateScript.CONTEXT.name, TemplateScript.CONTEXT);
        mockScriptService = new ScriptService(Settings.EMPTY, engines, contexts);
        apiKeyService = new ApiKeyService(Settings.EMPTY, mockCock, mockClient, mockSecurityIndexManager, mockClusterService,
                compositeRolesStore, mockScriptService, xContentRegistry());
    }

    public void testRoleDescriptorModifyDifferentCombinations() throws IOException {
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 3; col++) {
                final List<RoleDescriptor> subsetRoleDescriptors = SUBSET_RD_TEST_DATA.get(row);
                final List<RoleDescriptor> existingRoleDescriptors = EXISTING_RD_TEST_DATA.get(col);
                logger.info("---------------------Test case combination : (" + row + ", " + col + ")---------------------");
                logger.info("Subset Role Descriptors:\n" + subsetRoleDescriptors);
                logger.info("Existing Role Descriptors:\n" + existingRoleDescriptors);
                logger.info(expectedResultsForCombination[row][col]);

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
                                    // no modification to the subset role descriptor
                                    assertThat(queryBuilder, instanceOf(MatchQueryBuilder.class));
                                    verifyMatchQueryField((MatchQueryBuilder) queryBuilder, "category",
                                            inner.categoryValuesExpectedInQueryFromSubsetRDs);
                                } else {
                                    assertThat(queryBuilder, instanceOf(BoolQueryBuilder.class));
                                    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;

                                    // Verify should (from subset rd)
                                    assertThat(boolQueryBuilder.should().size(), is(1));
                                    assertThat(boolQueryBuilder.minimumShouldMatch(), is("1"));
                                    QueryBuilder shouldQueryBuilder = boolQueryBuilder.should().get(0);
                                    assertThat(shouldQueryBuilder, instanceOf(MatchQueryBuilder.class));
                                    verifyMatchQueryField((MatchQueryBuilder) shouldQueryBuilder, "category",
                                            inner.categoryValuesExpectedInQueryFromSubsetRDs[0]);

                                    // Verify filter (from existing rd)
                                    assertThat(boolQueryBuilder.filter().size(), is(1));
                                    QueryBuilder filterBoolQueryBuilder = boolQueryBuilder.filter().get(0);
                                    assertThat(filterBoolQueryBuilder, instanceOf(BoolQueryBuilder.class));
                                    BoolQueryBuilder filter = (BoolQueryBuilder) filterBoolQueryBuilder;

                                    assertThat(filter.should().size(), is(inner.categoryValuesExpectedInQueryFromExistingRDs.length));
                                    assertThat(filter.minimumShouldMatch(), is("1"));
                                    for (QueryBuilder qb : filter.should()) {
                                        assertThat(qb, instanceOf(MatchQueryBuilder.class));
                                        verifyMatchQueryField((MatchQueryBuilder) qb, "category",
                                                inner.categoryValuesExpectedInQueryFromExistingRDs);
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

    private void verifyMatchQueryField(MatchQueryBuilder matchQB, String expectedFieldName, String... expectedInValues) {
        String fieldName = matchQB.fieldName();
        assertThat(fieldName, equalTo(expectedFieldName));
        String value = (String) matchQB.value();
        assertThat(value, isIn(expectedInValues));
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

    // Matrix of result cases and expected data for different combinations from 
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

    private static List<List<RoleDescriptor>> initializeExistingRoleDescriptorsTestData() {
        List<List<RoleDescriptor>> existingRDTestData = new ArrayList<>();
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
        return existingRDTestData;
    }

    private static List<List<RoleDescriptor>> initializeSubsetRoleDescriptorsTestData() {
        List<List<RoleDescriptor>> subsetRDTestData = new ArrayList<>();
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
        return subsetRDTestData;
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
