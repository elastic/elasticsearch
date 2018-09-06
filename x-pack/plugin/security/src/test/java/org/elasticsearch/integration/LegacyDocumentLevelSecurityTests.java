/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.aggregations.Children;
import org.elasticsearch.join.aggregations.JoinAggregationBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class LegacyDocumentLevelSecurityTests extends SecurityIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSecurity.class, CommonAnalysisPlugin.class, ParentJoinPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() +
                "user1:" + usersPasswdHashed + "\n" +
                "user2:" + usersPasswdHashed + "\n" +
                "user3:" + usersPasswdHashed + "\n" +
                "user4:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1,user2,user3\n" +
                "role2:user1,user3\n" +
                "role3:user2,user3\n" +
                "role4:user4\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: [ none ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ none ]\n" +
                "\nrole2:\n" +
                "  cluster:\n" +
                "    - all\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges:\n" +
                "        - all\n" +
                "      query: \n" +
                "        term: \n" +
                "          field1: value1\n" +
                "role3:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'\n" + // <-- query defined as json in a string
                "role4:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                // query that can match nested documents
                "      query: '{\"bool\": { \"must_not\": { \"term\" : {\"field1\" : \"value2\"}}}}'";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
                .put(XPackSettings.AUDIT_ENABLED.getKey(), false) // Just to make logs less noisy
                .build();
    }

    public void testChildrenAggregation() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
                .addMapping("type2", "_parent", "type=type1", "field3", "type=text,fielddata=true")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type2", "2").setSource("field3", "value3")
                .setParent("1")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse response = client().prepareSearch("test")
                .setTypes("type1")
                .addAggregation(JoinAggregationBuilders.children("children", "type2")
                        .subAggregation(AggregationBuilders.terms("field3").field("field3")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        Children children = response.getAggregations().get("children");
        assertThat(children.getDocCount(), equalTo(1L));
        Terms termsAgg = children.getAggregations().get("field3");
        assertThat(termsAgg.getBuckets().get(0).getKeyAsString(), equalTo("value3"));
        assertThat(termsAgg.getBuckets().get(0).getDocCount(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setTypes("type1")
                .addAggregation(JoinAggregationBuilders.children("children", "type2")
                        .subAggregation(AggregationBuilders.terms("field3").field("field3")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        children = response.getAggregations().get("children");
        assertThat(children.getDocCount(), equalTo(0L));
        termsAgg = children.getAggregations().get("field3");
        assertThat(termsAgg.getBuckets().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setTypes("type1")
                .addAggregation(JoinAggregationBuilders.children("children", "type2")
                        .subAggregation(AggregationBuilders.terms("field3").field("field3")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        children = response.getAggregations().get("children");
        assertThat(children.getDocCount(), equalTo(0L));
        termsAgg = children.getAggregations().get("field3");
        assertThat(termsAgg.getBuckets().size(), equalTo(0));
    }

    public void testParentChild_parentField() {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent", "field1", "type=text", "field2", "type=text", "field3", "type=text"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("field1", "value1").get();
        client().prepareIndex("test", "child", "c1").setSource("field2", "value2").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("field2", "value2").setParent("p1").get();
        client().prepareIndex("test", "child", "c3").setSource("field3", "value3").setParent("p1").get();
        refresh();
        DocumentLevelSecurityTests.verifyParentChild();
    }

}
