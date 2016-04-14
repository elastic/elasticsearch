/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class DateMathExpressionIntegTests extends ShieldIntegTestCase {

    protected static final SecuredString USERS_PASSWD = new SecuredString("change_me".toCharArray());
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(USERS_PASSWD));

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "user1:" + USERS_PASSWD_HASHED + "\n";
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
        final String expectedIndexName = new IndexNameExpressionResolver(Settings.EMPTY).resolveDateMathExpression(expression);
        final boolean refeshOnOperation = randomBoolean();
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", basicAuthHeaderValue("user1", USERS_PASSWD)));

        if (randomBoolean()) {
            CreateIndexResponse response = client.admin().indices().prepareCreate(expression).get();
            assertThat(response.isAcknowledged(), is(true));
        }
        IndexResponse response = client.prepareIndex(expression, "type").setSource("foo", "bar").setRefresh(refeshOnOperation).get();

        assertThat(response.isCreated(), is(true));
        assertThat(response.getIndex(), containsString(expectedIndexName));

        if (refeshOnOperation == false) {
            client.admin().indices().prepareRefresh(expression).get();
        }
        SearchResponse searchResponse = client.prepareSearch(expression)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        assertThat(searchResponse.getHits().getTotalHits(), is(1L));

        MultiSearchResponse multiSearchResponse = client.prepareMultiSearch()
                .add(client.prepareSearch(expression).setQuery(QueryBuilders.matchAllQuery()).request())
                .get();
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));

        UpdateResponse updateResponse = client.prepareUpdate(expression, "type", response.getId())
                .setDoc("new", "field")
                .setRefresh(refeshOnOperation)
                .get();
        assertThat(updateResponse.isCreated(), is(false));

        if (refeshOnOperation == false) {
            client.admin().indices().prepareRefresh(expression).get();
        }
        GetResponse getResponse = client.prepareGet(expression, "type", response.getId()).setFetchSource(true).get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getSourceAsMap().get("foo").toString(), is("bar"));
        assertThat(getResponse.getSourceAsMap().get("new").toString(), is("field"));

        // multi get doesn't support expressions - this is probably a bug
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add(expression, "type", response.getId())
                .get();
        assertThat(multiGetResponse.getResponses()[0].getFailure().getMessage(), is("no such index"));

        DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(expression).get();
        assertThat(deleteIndexResponse.isAcknowledged(), is(true));
    }

}
