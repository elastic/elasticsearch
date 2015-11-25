/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchGetAndSuggestPermissionsTests extends ShieldIntegTestCase {
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("passwd".toCharArray())));

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" +
                "\n" +
                "search_role:\n" +
                "  indices:\n" +
                "    'a': search\n" +
                "\n" +
                "get_role:\n" +
                "  indices:\n" +
                "    'a': get\n" +
                "\n" +
                "suggest_role:\n" +
                "  indices:\n" +
                "    'a': suggest\n";
    }

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "search_user:" + USERS_PASSWD_HASHED + "\n" +
                "get_user:" + USERS_PASSWD_HASHED + "\n" +
                "suggest_user:" + USERS_PASSWD_HASHED + "\n";

    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "search_role:search_user\n" +
                "get_role:get_user\n" +
                "suggest_role:suggest_user\n";
    }

    /**
     * testing both "search" and "suggest" privileges can execute the suggest API
     */
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
        } catch (ElasticsearchSecurityException e) {
            logger.error("failed to search", e);
            // expected
        }
    }

    /**
     * testing that "search" privilege cannot execute the get API
     */
    public void testGetAPI() throws Exception {
        IndexResponse indexResponse = index("a", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        try {
            client.prepareGet("a", "type", indexResponse.getId())
                    .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("search_user", "passwd"))
                    .get();
            fail("a user with only search privilege should not be authorized for a get request");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthorizationException(e);
            logger.error("could not get document", e);
        }
    }

    /**
     * testing that "get" privilege can execute the mget API, and "search" privilege cannot execute mget
     */
    public void testMultiGetAPI() throws Exception {
        IndexResponse indexResponse = index("a", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        MultiGetResponse response = client.prepareMultiGet().add("a", "type", indexResponse.getId())
                .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("get_user", "passwd"))
                .get();
        assertNotNull(response);
        assertThat(response.getResponses().length, is(1));
        assertThat(response.getResponses()[0].getId(), equalTo(indexResponse.getId()));

        try {
            client.prepareMultiGet().add("a", "type", indexResponse.getId())
                    .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("search_user", "passwd"))
                    .get();
            fail("a user with only a search privilege should not be able to execute the mget API");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthorizationException(e);
            logger.error("could not mget documents", e);
        }
    }

    /**
     * testing that "search" privilege can execute the msearch API
     */
    public void testMultiSearchAPI() throws Exception {
        IndexResponse indexResponse = index("a", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        MultiSearchResponse response = client.prepareMultiSearch().add(searchRequest("a").types("type"))
                .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, userHeader("search_user", "passwd"))
                .get();
        assertNotNull(response);
        assertThat(response.getResponses().length, is(1));
        SearchResponse first = response.getResponses()[0].getResponse();
        assertNotNull(first);
        assertNoFailures(first);
    }

    private static String userHeader(String username, String password) {
        return UsernamePasswordToken.basicAuthHeaderValue(username, SecuredStringTests.build(password));
    }
}
