/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class DateMathExpressionIntegTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));

        return super.configUsers() +
            "user1:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: [ none ]\n" +
                "  indices:\n" +
                "    - names: 'datemath-*'\n" +
                "      privileges: [ ALL ]\n";
    }

    public void testDateMathExpressionsCanBeAuthorized() throws Exception {
        final String expression = "<datemath-{now/M}>";
        final String expectedIndexName = new IndexNameExpressionResolver().resolveDateMathExpression(expression);
        final boolean refeshOnOperation = randomBoolean();
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", basicAuthHeaderValue("user1", USERS_PASSWD)));

        if (randomBoolean()) {
            CreateIndexResponse response = client.admin().indices().prepareCreate(expression).get();
            assertThat(response.isAcknowledged(), is(true));
        }
        IndexResponse response = client.prepareIndex(expression).setSource("foo", "bar")
                .setRefreshPolicy(refeshOnOperation ? IMMEDIATE : NONE).get();

        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
        assertThat(response.getIndex(), containsString(expectedIndexName));

        if (refeshOnOperation == false) {
            client.admin().indices().prepareRefresh(expression).get();
        }
        SearchResponse searchResponse = client.prepareSearch(expression)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, is(1L));

        MultiSearchResponse multiSearchResponse = client.prepareMultiSearch()
                .add(client.prepareSearch(expression).setQuery(QueryBuilders.matchAllQuery()).request())
                .get();
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, is(1L));

        UpdateResponse updateResponse = client.prepareUpdate(expression, response.getId())
                .setDoc(Requests.INDEX_CONTENT_TYPE, "new", "field")
                .setRefreshPolicy(refeshOnOperation ? IMMEDIATE : NONE)
                .get();
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());

        if (refeshOnOperation == false) {
            client.admin().indices().prepareRefresh(expression).get();
        }
        GetResponse getResponse = client.prepareGet(expression, response.getId()).setFetchSource(true).get();
        assertThat(getResponse.isExists(), is(true));
        assertEquals(expectedIndexName, getResponse.getIndex());
        assertThat(getResponse.getSourceAsMap().get("foo").toString(), is("bar"));
        assertThat(getResponse.getSourceAsMap().get("new").toString(), is("field"));

        // multi get doesn't support expressions - this is probably a bug
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add(expression, response.getId())
                .get();
        assertFalse(multiGetResponse.getResponses()[0].isFailed());
        assertTrue(multiGetResponse.getResponses()[0].getResponse().isExists());
        assertEquals(expectedIndexName, multiGetResponse.getResponses()[0].getResponse().getIndex());


        AcknowledgedResponse deleteIndexResponse = client.admin().indices().prepareDelete(expression).get();
        assertThat(deleteIndexResponse.isAcknowledged(), is(true));
    }

}
