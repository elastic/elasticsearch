/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MultipleIndicesPermissionsTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        // adds a dummy user to the native realm to force .security index creation
        new TestSecurityClient(getRestClient(), SecuritySettingsSource.SECURITY_REQUEST_OPTIONS).putUser(
            new User("dummy_user", "missing_role"),
            SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
        );
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
        // The definition of TEST_ROLE here is intentionally different than the definition in the superclass.
        return Strings.format("""
            %s:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [manage]
                - names: '/.*/'
                  privileges: [write]
                - names: 'test'
                  privileges: [read]
                - names: 'test1'
                  privileges: [read]

            role_a:
              indices:
                - names: 'a'
                  privileges: [all]
                - names: 'alias1'
                  privileges: [read]

            role_monitor_all_unrestricted_indices:
              cluster: [monitor]
              indices:
                - names: '*'
                  privileges: [monitor]

            role_b:
              indices:
                - names: 'b'
                  privileges: [all]
            """, SecuritySettingsSource.TEST_ROLE) + '\n' + SecuritySettingsSourceField.ES_TEST_ROOT_ROLE_YML;
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return SecuritySettingsSource.CONFIG_STANDARD_USER
            + "user_a:"
            + usersPasswdHashed
            + "\n"
            + "user_ab:"
            + usersPasswdHashed
            + "\n"
            + "user_monitor:"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return SecuritySettingsSource.CONFIG_STANDARD_USER_ROLES + """
            role_a:user_a,user_ab
            role_b:user_ab
            role_monitor_all_unrestricted_indices:user_monitor
            """;
    }

    public void testSingleRole() throws Exception {
        DocWriteResponse indexResponse = index("test", jsonBuilder().startObject().field("name", "value").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("test1", jsonBuilder().startObject().field("name", "value1").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        Client client = client();

        // no specifying an index, should replace indices with the permitted ones (test & test1)
        assertHitCountAndNoFailures(prepareSearch().setQuery(matchAllQuery()), 2);

        // _all should expand to all the permitted indices
        assertHitCountAndNoFailures(client.prepareSearch("_all").setQuery(matchAllQuery()), 2);

        // wildcards should expand to all the permitted indices
        assertHitCountAndNoFailures(client.prepareSearch("test*").setQuery(matchAllQuery()), 2);

        try {
            client.prepareSearch("test", "test2").setQuery(matchAllQuery()).get();
            fail("expected an authorization exception when one of multiple indices is forbidden");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }

        MultiSearchResponse msearchResponse = client.prepareMultiSearch()
            .add(client.prepareSearch("test"))
            .add(client.prepareSearch("test1"))
            .get();
        try {
            MultiSearchResponse.Item[] items = msearchResponse.getResponses();
            assertThat(items.length, is(2));
            assertThat(items[0].isFailure(), is(false));
            var searchResponse = items[0].getResponse();
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, 1);
            assertThat(items[1].isFailure(), is(false));
            searchResponse = items[1].getResponse();
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, 1);
        } finally {
            msearchResponse.decRef();
        }
    }

    public void testMonitorRestrictedWildcards() throws Exception {

        DocWriteResponse indexResponse = index("foo", jsonBuilder().startObject().field("name", "value").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("foobar", jsonBuilder().startObject().field("name", "value").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("foobarfoo", jsonBuilder().startObject().field("name", "value").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        final Client client = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_monitor", USERS_PASSWD))
        );

        final GetSettingsResponse getSettingsResponse = client.admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, randomFrom("*", "_all", "foo*"))
            .get();
        assertThat(getSettingsResponse.getIndexToSettings().size(), is(3));
        assertThat(getSettingsResponse.getIndexToSettings().containsKey("foo"), is(true));
        assertThat(getSettingsResponse.getIndexToSettings().containsKey("foobar"), is(true));
        assertThat(getSettingsResponse.getIndexToSettings().containsKey("foobarfoo"), is(true));

        final IndicesShardStoresResponse indicesShardsStoresResponse = client.execute(
            TransportIndicesShardStoresAction.TYPE,
            new IndicesShardStoresRequest(randomFrom("*", "_all", "foo*")).shardStatuses("all")
        ).actionGet(10, TimeUnit.SECONDS);
        assertThat(indicesShardsStoresResponse.getStoreStatuses().size(), is(3));
        assertThat(indicesShardsStoresResponse.getStoreStatuses().containsKey("foo"), is(true));
        assertThat(indicesShardsStoresResponse.getStoreStatuses().containsKey("foobar"), is(true));
        assertThat(indicesShardsStoresResponse.getStoreStatuses().containsKey("foobarfoo"), is(true));

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
        DocWriteResponse indexResponse = index("a", jsonBuilder().startObject().field("name", "value_a").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = index("b", jsonBuilder().startObject().field("name", "value_b").endObject());
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        refresh();

        Client client = client();

        assertHitCountAndNoFailures(
            client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD)))
                .prepareSearch("a"),
            1
        );

        String[] indices = randomDouble() < 0.3 ? new String[] { "_all" } : randomBoolean() ? new String[] { "*" } : new String[] {};
        assertHitCountAndNoFailures(
            client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD)))
                .prepareSearch(indices),
            1
        );

        try {
            indices = randomBoolean() ? new String[] { "a", "b" } : new String[] { "b", "a" };
            client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD)))
                .prepareSearch(indices)
                .get();
            fail(
                "expected an authorization excpetion when trying to search on multiple indices where there are no search permissions on "
                    + "one/some of them"
            );
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }

        assertHitCountAndNoFailures(
            client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", USERS_PASSWD)))
                .prepareSearch("b"),
            1
        );

        indices = randomBoolean() ? new String[] { "a", "b" } : new String[] { "b", "a" };
        assertHitCountAndNoFailures(
            client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", USERS_PASSWD)))
                .prepareSearch(indices),
            2
        );

        indices = randomDouble() < 0.3 ? new String[] { "_all" } : randomBoolean() ? new String[] { "*" } : new String[] {};
        assertHitCountAndNoFailures(
            client.filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_ab", USERS_PASSWD)))
                .prepareSearch(indices),
            2
        );
    }

    public void testMultiNamesWorkCorrectly() {
        assertAcked(
            indicesAdmin().prepareCreate("index1")
                .setMapping("field1", "type=text")
                .addAlias(new Alias("alias1").filter(QueryBuilders.termQuery("field1", "public")))
        );

        prepareIndex("index1").setId("1").setSource("field1", "private").setRefreshPolicy(IMMEDIATE).get();

        final Client userAClient = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_a", USERS_PASSWD))
        );

        assertResponse(
            userAClient.prepareSearch("alias1").setSize(0),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(0L))
        );

        final ElasticsearchSecurityException e1 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> userAClient.prepareSearch("index1").get()
        );
        assertThat(e1.getMessage(), containsString("is unauthorized for user [user_a]"));

        // The request should fail because index1 is directly requested and is not authorized for user_a
        final ElasticsearchSecurityException e2 = expectThrows(
            ElasticsearchSecurityException.class,
            () -> userAClient.prepareSearch("index1", "alias1").get()
        );
        assertThat(e2.getMessage(), containsString("is unauthorized for user [user_a]"));
    }
}
