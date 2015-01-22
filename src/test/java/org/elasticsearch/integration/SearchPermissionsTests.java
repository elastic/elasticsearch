/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.SUITE)
public class SearchPermissionsTests extends ShieldIntegrationTest {

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" +
                "\n" +
                "suggest_role:\n" +
                "  indices:\n" +
                "    'a': suggest\n" +
                "\n" +
                "search_role:\n" +
                "  indices:\n" +
                "    'a': search\n" +
                "\n" +
                "get_role:\n" +
                "  indices:\n" +
                "    'a': get\n";
    }

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "suggest_user:{plain}passwd\n" +
                "search_user:{plain}passwd\n" +
                "get_user:{plain}passwd\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "suggest_role:suggest_user\n" +
                "search_role:search_user\n" +
                "get_role:get_user";
    }

    /**
     * testing both "search" and "suggest" privileges can execute the suggest API
     */
    @Test
     public void testSuggestAPI() throws Exception {
        IndexResponse indexResponse = index("a", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        SuggestResponse suggestResponse = client.prepareSuggest("a")
                .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("suggest_user", "passwd"))
                .addSuggestion(SuggestBuilders.termSuggestion("name").field("name").text("val")).get();
        assertNoFailures(suggestResponse);
        assertThat(suggestResponse.getSuggest().size(), is(1));

        suggestResponse = client.prepareSuggest("a")
                .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("search_user", "passwd"))
                .addSuggestion(SuggestBuilders.termSuggestion("name").field("name").text("val")).get();
        assertNoFailures(suggestResponse);
        assertThat(suggestResponse.getSuggest().size(), is(1));

        try {
            client.prepareSearch("a")
                    .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("suggest_user", "passwd"))
                    .get();
            fail("a user with only a suggest privilege cannot execute search");
        } catch (AuthorizationException e) {
            logger.error("failed to search", e);
            // expected
        }
    }

    /**
     * testing both "search" and "get" privileges can execute the get API
     */
    @Test
    public void testGetAPI() throws Exception {
        IndexResponse indexResponse = index("a", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        GetResponse getResponse = client.prepareGet("a", "type", indexResponse.getId())
                .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("get_user", "passwd"))
                .get();
        assertNotNull(getResponse);
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        getResponse = client.prepareGet("a", "type", indexResponse.getId())
                .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("search_user", "passwd"))
                .get();
        assertNotNull(getResponse);
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        try {
            client.prepareSearch("a")
                    .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("get_user", "passwd"))
                    .get();
            fail("a user with only a get privilege cannot execute search");
        } catch (AuthorizationException e) {
            logger.error("failed to search", e);
            // expected
        }
    }

    private static String userHeader(String username, String password) {
        return UsernamePasswordToken.basicAuthHeaderValue(username, SecuredStringTests.build(password));
    }
}
