/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ShieldCachePermissionTests extends ShieldIntegrationTest {

    static final String READ_ONE_IDX_USER = "read_user";

    @Override
    public String configUsers() {
        return super.configUsers()
                + READ_ONE_IDX_USER + ":" + ShieldSettingsSource.DEFAULT_PASSWORD_HASHED + "\n";
    }

    @Override
    public String configRoles() {
        return super.configRoles()
                + "\nread_one_idx:\n"
                + "  indices:\n"
                + "    'data': READ\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles()
                + "read_one_idx:" + READ_ONE_IDX_USER + "\n";
    }

    @BeforeClass
    public static void checkVersion() {
        assumeTrue("These tests are only valid with elasticsearch 1.6.0+", Version.CURRENT.id >= 1060099);
    }

    @Before
    public void loadData() {
        index("data", "a", "1", "{ \"name\": \"John\", \"token\": \"token1\" }");
        index("tokens", "tokens", "1", "{ \"group\": \"1\", \"tokens\": [\"token1\", \"token2\"] }");
        client().preparePutIndexedScript().setOpType(IndexRequest.OpType.CREATE).setSource("{\n" +
                "\"template\": {\n" +
                "  \"query\": {\n" +
                "    \"filtered\": {\n" +
                "      \"filter\": {\n" +
                "        \"exists\": {\n" +
                "          \"field\": \"{{name}}\"\n" +
                "         }\n" +
                "       }\n" +
                "     }\n" +
                "   }\n" +
                " }\n" +
                "}")
                .setScriptLang("mustache")
                .setId("testTemplate")
                .execute().actionGet();
        refresh();
    }

    @Test
    public void testThatTermsFilterQueryDoesntLeakData() {
        SearchResponse response = client().prepareSearch("data").setTypes("a").setQuery(QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                QueryBuilders.termsLookupQuery("token")
                        .lookupIndex("tokens")
                        .lookupType("tokens")
                        .lookupId("1")
                        .lookupPath("tokens")))
                .execute().actionGet();
        assertThat(response.isTimedOut(), is(false));
        assertThat(response.getHits().hits().length, is(1));

        // Repeat with unauthorized user!!!!
        try {
            client().prepareSearch("data").setTypes("a").setQuery(QueryBuilders.filteredQuery(
                    QueryBuilders.matchAllQuery(),
                    QueryBuilders.termsLookupQuery("token")
                            .lookupIndex("tokens")
                            .lookupType("tokens")
                            .lookupId("1")
                            .lookupPath("tokens")))
                    .putHeader("Authorization", basicAuthHeaderValue(READ_ONE_IDX_USER, new SecuredString("changeme".toCharArray())))
                    .execute().actionGet();
            fail("search phase exception should have been thrown");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("AuthorizationException"));
        }
    }

    @Test
    public void testThatScriptServiceDoesntLeakData() {
        SearchResponse response = client().prepareSearch("data").setTypes("a")
                .setTemplateType(ScriptService.ScriptType.INDEXED)
                .setTemplateName("testTemplate")
                .setTemplateParams(Collections.<String, Object>singletonMap("name", "token"))
                .execute().actionGet();
        assertThat(response.isTimedOut(), is(false));
        assertThat(response.getHits().hits().length, is(1));

        // Repeat with unauthorized user!!!!
        try {
            client().prepareSearch("data").setTypes("a")
                    .setTemplateType(ScriptService.ScriptType.INDEXED)
                    .setTemplateName("testTemplate")
                    .setTemplateParams(Collections.<String, Object>singletonMap("name", "token"))
                    .putHeader("Authorization", basicAuthHeaderValue(READ_ONE_IDX_USER, new SecuredString("changeme".toCharArray())))
                    .execute().actionGet();
            fail("search phase exception should have been thrown");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("AuthorizationException"));
        }
    }
}
