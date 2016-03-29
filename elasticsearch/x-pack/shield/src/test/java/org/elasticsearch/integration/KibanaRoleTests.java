/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class KibanaRoleTests extends ShieldIntegTestCase {

    protected static final SecuredString USERS_PASSWD = new SecuredString("change_me".toCharArray());
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("change_me".toCharArray())));

    @Override
    public String configRoles() {
        return super.configRoles() + "\n" +
                "my_kibana_user:\n" +
                "  cluster:\n" +
                "      - monitor\n" +
                "  indices:\n" +
                "    - names: 'logstash-*'\n" +
                "      privileges:\n" +
                "        - view_index_metadata\n" +
                "        - read\n" +
                "    - names: '.kibana*'\n" +
                "      privileges:\n" +
                "        - manage\n" +
                "        - read\n" +
                "        - index";
    }

    @Override
    public String configUsers() {
        return super.configUsers() +
                "kibana_user:" + USERS_PASSWD_HASHED;
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() +
                "my_kibana_user:kibana_user";
    }

    public void testFieldMappings() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, client().prepareIndex().setIndex(index).setType(type).setSource(field, "bar"));

        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings().addIndices("logstash-*").setFields("*")
                .includeDefaults(true).get();
        FieldMappingMetaData fieldMappingMetaData = response.fieldMappings(index, type, field);
        assertThat(fieldMappingMetaData, notNullValue());
        assertThat(fieldMappingMetaData.isNull(), is(false));

        response = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD)))
                .admin().indices().prepareGetFieldMappings().addIndices("logstash-*")
                .setFields("*")
                .includeDefaults(true).get();
        FieldMappingMetaData fieldMappingMetaData1 = response.fieldMappings(index, type, field);
        assertThat(fieldMappingMetaData1, notNullValue());
        assertThat(fieldMappingMetaData1.isNull(), is(false));
        assertThat(fieldMappingMetaData1.fullName(), equalTo(fieldMappingMetaData.fullName()));
    }

    public void testValidateQuery() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, client().prepareIndex().setIndex(index).setType(type).setSource(field, "bar"));

        ValidateQueryResponse response = client().admin().indices()
                .prepareValidateQuery(index).setQuery(QueryBuilders.termQuery(field, "bar")).get();
        assertThat(response.isValid(), is(true));

        response = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD)))
                .admin().indices()
                .prepareValidateQuery(index)
                .setQuery(QueryBuilders.termQuery(field, "bar")).get();
        assertThat(response.isValid(), is(true));
    }

    public void testSearchAndMSearch() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, client().prepareIndex().setIndex(index).setType(type).setSource(field, "bar"));

        SearchResponse response = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
        final long hits = response.getHits().totalHits();
        assertThat(hits, greaterThan(0L));
        response = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD)))
                .prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery()).get();
        assertEquals(response.getHits().totalHits(), hits);


        MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())).get();
        final long multiHits = multiSearchResponse.getResponses()[0].getResponse().getHits().totalHits();
        assertThat(hits, greaterThan(0L));
        multiSearchResponse = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())).get();
        assertEquals(multiSearchResponse.getResponses()[0].getResponse().getHits().totalHits(), multiHits);
    }

    public void testFieldStats() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, client().prepareIndex().setIndex(index).setType(type).setSource(field, "bar"));

        FieldStatsResponse response = client().prepareFieldStats().setIndices(index).setFields(field).get();
        FieldStats fieldStats = response.getAllFieldStats().get(field);
        assertThat(fieldStats, notNullValue());
        final String fieldStatsMax = fieldStats.getMaxValueAsString();

        response = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD)))
                .prepareFieldStats()
                .setIndices(index).setFields(field).get();
        FieldStats fieldStats1 = response.getAllFieldStats().get(field);
        assertThat(fieldStats1, notNullValue());
        assertThat(fieldStats1.getMaxValueAsString(), equalTo(fieldStatsMax));
    }

    public void testGetIndex() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, client().prepareIndex().setIndex(index).setType(type).setSource(field, "bar"));

        GetIndexResponse response = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertThat(response.getIndices(), arrayContaining(index));

        response = client()
                .filterWithHeader(singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD)))
                .admin().indices().prepareGetIndex()
                .setIndices(index).get();
        assertThat(response.getIndices(), arrayContaining(index));
    }
}
