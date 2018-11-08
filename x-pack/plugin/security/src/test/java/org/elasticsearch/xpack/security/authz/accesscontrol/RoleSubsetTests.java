/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RoleSubsetTests extends AbstractBuilderTestCase {

    public void testIsSubsetOnTheRoleAndThenVerifyModifiedRoleDescriptorsForDLSCombinedQuery() throws IOException {
        // Base role descriptors
        IndicesPrivileges baseRD1IP1 = IndicesPrivileges.builder()
                .indices("index-1-1-*")
                .privileges("READ")
                .query("{ \"match\": { \"category\": \"BRD1\" } }")
                .build();
        RoleDescriptor baseRD1 = new RoleDescriptor("base-rd-1", null, new IndicesPrivileges[] { baseRD1IP1 }, null);
        IndicesPrivileges baseRD2IP1 = IndicesPrivileges.builder()
                .indices("index-1-1-1-*", "index-1-1-2-*")
                .privileges("READ")
                .query("{ \"match\": { \"category\": \"BRD2\" } }")
                .build();
        RoleDescriptor baseRD2 = new RoleDescriptor("base-rd-2", null, new IndicesPrivileges[] { baseRD2IP1 }, null);

        final PlainActionFuture<Role> future1 = new PlainActionFuture<>();
        final Set<RoleDescriptor> baseDescriptors = Sets.newHashSet(baseRD1, baseRD2);
        CompositeRolesStore.buildRoleFromDescriptors(baseDescriptors, new FieldPermissionsCache(Settings.EMPTY), null, future1);
        Role baseRole = future1.actionGet();

        // Child role descriptors
        IndicesPrivileges rd1IP1 = IndicesPrivileges.builder()
                .indices("index-1-1-1-*")
                .privileges("READ")
                .query("{ \"match\": { \"category\": \"RD1\" } }")
                .build();
        RoleDescriptor rd1 = new RoleDescriptor("rd-1", null, new IndicesPrivileges[] { rd1IP1 }, null);
        IndicesPrivileges rd2IP1 = IndicesPrivileges.builder()
                .indices("index-1-1-2-*")
                .privileges("READ")
                .query("{ \"match\": { \"category\": \"RD2\" } }")
                .build();
        RoleDescriptor rd2 = new RoleDescriptor("rd-2", null, new IndicesPrivileges[] { rd2IP1 }, null);

        final PlainActionFuture<Role> future2 = new PlainActionFuture<>();
        final Set<RoleDescriptor> childDescriptors = Sets.newHashSet(rd1, rd2);
        CompositeRolesStore.buildRoleFromDescriptors(childDescriptors, new FieldPermissionsCache(Settings.EMPTY), null, future2);
        Role role = future2.actionGet();

        // Check if it is a subset
        SubsetResult result = role.isSubsetOf(baseRole);
        assertThat(result.result(), equalTo(SubsetResult.Result.MAYBE));
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("index-1-1-1-*"));
        expected.add(Sets.newHashSet("index-1-1-2-*"));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(expected));

        // This is what I need to do next to update the child role descriptors:
        Map<Set<String>, BoolQueryBuilder> indexNamePatternsToBoolQueryBuilder = new HashMap<>();
        for (Set<String> indexNamePattern : result.setOfIndexNamesForCombiningDLSQueries()) {
            BoolQueryBuilder parentFilterQueryBuilder = QueryBuilders.boolQuery();
            // Now find the index name patterns from all base descriptors that
            // match and combine queries
            for (RoleDescriptor rdbase : baseDescriptors) {
                for (IndicesPrivileges ip : rdbase.getIndicesPrivileges()) {
                    if (Operations.subsetOf(Automatons.patterns(indexNamePattern), Automatons.patterns(ip.getIndices()))) {
                        // TODO handle if the query is template
                        parentFilterQueryBuilder.should(
                                AbstractQueryBuilder.parseInnerQueryBuilder(createParser(XContentType.JSON.xContent(), ip.getQuery())));
                    }
                }
            }
            parentFilterQueryBuilder.minimumShouldMatch(1);
            BoolQueryBuilder finalBoolQueryBuilder = QueryBuilders.boolQuery();
            finalBoolQueryBuilder.filter(parentFilterQueryBuilder);
            finalBoolQueryBuilder.minimumShouldMatch(1);
            // Iterate on child role descriptors and combine queries if the
            // index name patterns match.
            for (RoleDescriptor childRD : childDescriptors) {
                for (IndicesPrivileges ip : childRD.getIndicesPrivileges()) {
                    if (Sets.newHashSet(ip.getIndices()).equals(indexNamePattern)) {
                        // TODO handle if the query is template
                        finalBoolQueryBuilder.should(
                                AbstractQueryBuilder.parseInnerQueryBuilder(createParser(XContentType.JSON.xContent(), ip.getQuery())));
                    }
                }
            }
            indexNamePatternsToBoolQueryBuilder.put(indexNamePattern, finalBoolQueryBuilder);
        }

        final Set<RoleDescriptor> newChildDescriptors = new HashSet<>();
        for (RoleDescriptor childRD : childDescriptors) {
            Set<IndicesPrivileges> updates = new HashSet<>();
            for (IndicesPrivileges ip : childRD.getIndicesPrivileges()) {
                Set<String> indices = Sets.newHashSet(ip.getIndices());
                if (indexNamePatternsToBoolQueryBuilder.get(indices) != null) {
                    BoolQueryBuilder boolQueryBuilder = indexNamePatternsToBoolQueryBuilder.get(indices);
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    boolQueryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    updates.add(IndicesPrivileges.builder()
                            .indices(ip.getIndices())
                            .privileges(ip.getPrivileges())
                            .grantedFields(ip.getGrantedFields())
                            .deniedFields(ip.getDeniedFields())
                            .query(new BytesArray(Strings.toString(builder)))
                            .build());
                } else {
                    updates.add(ip);
                }
            }
            RoleDescriptor rd = new RoleDescriptor(childRD.getName(), childRD.getClusterPrivileges(),
                    updates.toArray(new IndicesPrivileges[0]), childRD.getApplicationPrivileges(),
                    childRD.getConditionalClusterPrivileges(), childRD.getRunAs(), childRD.getMetadata(), childRD.getTransientMetadata());
            newChildDescriptors.add(rd);
        }

        assertThat(newChildDescriptors.size(), equalTo(2));

        final PlainActionFuture<Role> future3 = new PlainActionFuture<>();
        CompositeRolesStore.buildRoleFromDescriptors(newChildDescriptors, new FieldPermissionsCache(Settings.EMPTY), null, future3);
        Role finalRole = future3.actionGet();
        IndexMetaData.Builder imbBuilder = IndexMetaData
                .builder("index-1-1-1-1").settings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .putAlias(AliasMetaData.builder("_1111"));
        MetaData md = MetaData.builder().put(imbBuilder).build();
        IndicesAccessControl iac = finalRole.authorize(SearchAction.NAME, Sets.newHashSet("index-1-1-1-1"), md,
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

}
