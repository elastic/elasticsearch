/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class MultipleIndicesPermissionsTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("passwd".toCharArray());

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        // adds a dummy user to the native realm to force .security index creation
        new TestRestHighLevelClient().security().putUser(
            PutUserRequest.withPassword(new User("dummy_user", List.of("missing_role")), "password".toCharArray(), true,
                RefreshPolicy.IMMEDIATE), SECURITY_REQUEST_OPTIONS);
        assertSecurityIndexActive();
    }

    @After
    public void cleanupSecurityIndex() throws Exception {
        super.deleteSecurityIndex();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected String configRoles() {
        return SecuritySettingsSource.TEST_ROLE + ":\n" +
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
                "role_monitor_all_unrestricted_indices:\n" +
                "  cluster: [monitor]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [monitor]\n" +
                "\n" +
                "role_b:\n" +
                "  indices:\n" +
                "    - names: 'b'\n" +
                "      privileges: [all]\n";
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return SecuritySettingsSource.CONFIG_STANDARD_USER +
            "user_a:" + usersPasswdHashed + "\n" +
            "user_ab:" + usersPasswdHashed + "\n" +
            "user_monitor:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return SecuritySettingsSource.CONFIG_STANDARD_USER_ROLES +
                "role_a:user_a,user_ab\n" +
                "role_b:user_ab\n" +
                "role_monitor_all_unrestricted_indices:user_monitor\n";
    }

    public void testSingleRole() throws Exception {
        IndexResponse indexResponse = index("test", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());


        indexResponse = index("test1", jsonBuilder()
                .startObject()
                .field("name", "value1")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        Client client = client();

        // no specifying an index, should replace indices with the permitted ones (test & test1)
        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
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

    public void testMonitorRestrictedWildcards() throws Exception {

        IndexResponse indexResponse = index("foo", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("foobar", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("foobarfoo", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        final Client client = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_monitor", USERS_PASSWD)));

        final GetSettingsResponse getSettingsResponse = client.admin().indices().prepareGetSettings(randomFrom("*", "_all", "foo*")).get();
        assertThat(getSettingsResponse.getIndexToSettings().size(), is(3));
        assertThat(getSettingsResponse.getIndexToSettings().containsKey("foo"), is(true));
        assertThat(getSettingsResponse.getIndexToSettings().containsKey("foobar"), is(true));
        assertThat(getSettingsResponse.getIndexToSettings().containsKey("foobarfoo"), is(true));

        final IndicesShardStoresResponse indicesShardsStoresResponse = client.admin().indices()
                .prepareShardStores(randomFrom("*", "_all", "foo*")).setShardStatuses("all").get();
        assertThat(indicesShardsStoresResponse.getStoreStatuses().size(), is(3));
        assertThat(indicesShardsStoresResponse.getStoreStatuses().containsKey("foo"), is(true));
        assertThat(indicesShardsStoresResponse.getStoreStatuses().containsKey("foobar"), is(true));
        assertThat(indicesShardsStoresResponse.getStoreStatuses().containsKey("foobarfoo"), is(true));

        final UpgradeStatusResponse upgradeStatusResponse = client.admin().indices().prepareUpgradeStatus(randomFrom("*", "_all", "foo*"))
                .get();
        assertThat(upgradeStatusResponse.getIndices().size(), is(3));
        assertThat(upgradeStatusResponse.getIndices().keySet(), containsInAnyOrder("foo", "foobar", "foobarfoo"));

        final IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats(randomFrom("*", "_all", "foo*")).get();
        assertThat(indicesStatsResponse.getIndices().size(), is(3));
        assertThat(indicesStatsResponse.getIndices().keySet(), containsInAnyOrder("foo", "foobar", "foobarfoo"));

        final IndicesSegmentResponse indicesSegmentResponse = client.admin().indices().prepareSegments("*").get();
        assertThat(indicesSegmentResponse.getIndices().size(), is(3));
        assertThat(indicesSegmentResponse.getIndices().keySet(), containsInAnyOrder("foo", "foobar", "foobarfoo"));

        final RecoveryResponse indicesRecoveryResponse = client.admin().indices().prepareRecoveries("*").get();
        assertThat(indicesRecoveryResponse.shardRecoveryStates().size(), is(3));
        assertThat(indicesRecoveryResponse.shardRecoveryStates().keySet(), containsInAnyOrder("foo", "foobar", "foobarfoo"));

        // test _cat/indices with wildcards that cover unauthorized indices (".security" in this case)
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue("user_monitor", USERS_PASSWD));
        RequestOptions options = optionsBuilder.build();
        Request catIndicesRequest = new Request("GET", "/_cat/indices/" + randomFrom("*", "_all", "foo*"));
        catIndicesRequest.setOptions(options);
        Response catIndicesResponse = getRestClient().performRequest(catIndicesRequest);
        assertThat(catIndicesResponse.getStatusLine().getStatusCode() < 300, is(true));
    }

    public void testMultipleRoles() throws Exception {
        IndexResponse indexResponse = index("a", jsonBuilder()
                .startObject()
                .field("name", "value_a")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("b", jsonBuilder()
                .startObject()
                .field("name", "value_b")
                .endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        Client client = client();

        SearchResponse response = client
            .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD)))
                .prepareSearch("a")
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        String[] indices = randomDouble() < 0.3 ?
                new String[] { "_all"} : randomBoolean() ?
                new String[] { "*" } :
                new String[] {};
        response = client
            .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD)))
                .prepareSearch(indices)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        try {
            indices = randomBoolean() ? new String[] { "a", "b" } : new String[] { "b", "a" };
            client
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD)))
                    .prepareSearch(indices)
                    .get();
            fail("expected an authorization excpetion when trying to search on multiple indices where there are no search permissions on " +
                    "one/some of them");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }

        response = client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", USERS_PASSWD)))
                .prepareSearch("b")
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        indices = randomBoolean() ? new String[] { "a", "b" } : new String[] { "b", "a" };
        response = client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", USERS_PASSWD)))
                .prepareSearch(indices)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        indices = randomDouble() < 0.3 ?
                new String[] { "_all"} : randomBoolean() ?
                new String[] { "*" } :
                new String[] {};
        response = client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", USERS_PASSWD)))
                .prepareSearch(indices)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 2);
    }
}
