/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;

import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.indicesQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.is;

public class MultipleIndicesPermissionsTests extends ShieldIntegTestCase {
    protected static final SecuredString PASSWD = new SecuredString("passwd".toCharArray());
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(PASSWD));

    @Override
    protected String configRoles() {
        return ShieldSettingsSource.DEFAULT_ROLE + ":\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [manage]\n" +
                "    - names: '/.*/'\n" +
                "      privileges: [write]\n" +
                "    - names: 'test'\n" +
                "      privileges: [read]\n" +
                "    - names: 'test1'\n" +
                "      privileges: [read]\n" +
                "\n" +
                "role_a:\n" +
                "  indices:\n" +
                "    - names: 'a'\n" +
                "      privileges: [all]\n" +
                "\n" +
                "role_b:\n" +
                "  indices:\n" +
                "    - names: 'b'\n" +
                "      privileges: [all]\n";
    }

    @Override
    protected String configUsers() {
        return ShieldSettingsSource.CONFIG_STANDARD_USER +
                "user_a:" + USERS_PASSWD_HASHED + "\n" +
                "user_ab:" + USERS_PASSWD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return ShieldSettingsSource.CONFIG_STANDARD_USER_ROLES +
                "role_a:user_a,user_ab\n" +
                "role_b:user_ab\n";
    }

    public void testSingleRole() throws Exception {
        IndexResponse indexResponse = index("test", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));


        indexResponse = index("test1", "type", jsonBuilder()
                .startObject()
                .field("name", "value1")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        // no specifying an index, should replace indices with the permitted ones (test & test1)
        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        searchResponse = client.prepareSearch().setQuery(indicesQuery(matchAllQuery(), "test1")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        // _all should expand to all the permitted indices
        searchResponse = client.prepareSearch("_all").setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        // wildcards should expand to all the permitted indices
        searchResponse = client.prepareSearch("test*").setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        // specifying a permitted index, should only return results from that index
        searchResponse = client.prepareSearch("test1").setQuery(indicesQuery(matchAllQuery(), "test1")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        // specifying a forbidden index, should throw an authorization exception
        try {
            client.prepareSearch("test2").setQuery(indicesQuery(matchAllQuery(), "test1")).get();
            fail("expected an authorization exception when searching a forbidden index");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }

        try {
            client.prepareSearch("test", "test2").setQuery(matchAllQuery()).get();
            fail("expected an authorization exception when one of mulitple indices is forbidden");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }

        MultiSearchResponse msearchResponse = client.prepareMultiSearch()
                .add(client.prepareSearch("test"))
                .add(client.prepareSearch("test1"))
                .get();
        MultiSearchResponse.Item[] items = msearchResponse.getResponses();
        assertThat(items.length, is(2));
        assertThat(items[0].isFailure(), is(false));
        searchResponse = items[0].getResponse();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(items[1].isFailure(), is(false));
        searchResponse = items[1].getResponse();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);
    }

    public void testMultipleRoles() throws Exception {
        IndexResponse indexResponse = index("a", "type", jsonBuilder()
                .startObject()
                .field("name", "value_a")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        indexResponse = index("b", "type", jsonBuilder()
                .startObject()
                .field("name", "value_b")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        SearchResponse response = client
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", PASSWD)))
                .prepareSearch("a")
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        String[] indices = randomDouble() < 0.3 ?
                new String[] { "_all"} : randomBoolean() ?
                new String[] { "*" } :
                new String[] {};
        response = client
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", PASSWD)))
                .prepareSearch(indices)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        try {
            indices = randomBoolean() ? new String[] { "a", "b" } : new String[] { "b", "a" };
            client
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", PASSWD)))
                    .prepareSearch(indices)
                    .get();
            fail("expected an authorization excpetion when trying to search on multiple indices where there are no search permissions on " +
                    "one/some of them");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }

        response = client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", PASSWD)))
                .prepareSearch("b")
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        indices = randomBoolean() ? new String[] { "a", "b" } : new String[] { "b", "a" };
        response = client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", PASSWD)))
                .prepareSearch(indices)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        indices = randomDouble() < 0.3 ?
                new String[] { "_all"} : randomBoolean() ?
                new String[] { "*" } :
                new String[] {};
        response = client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", PASSWD)))
                .prepareSearch(indices)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 2);
    }
}
