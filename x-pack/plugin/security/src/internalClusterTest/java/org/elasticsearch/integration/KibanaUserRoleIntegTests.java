/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class KibanaUserRoleIntegTests extends NativeRealmIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    public String configRoles() {
        return Strings.format("""
            %s
            my_kibana_user:
              indices:
                - names: 'logstash-*'
                  privileges:
                    - view_index_metadata
                    - read
            """, super.configRoles());
    }

    @Override
    public String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() + "kibana_user:" + usersPasswdHashed;
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() + "my_kibana_user:kibana_user\n" + "kibana_user:kibana_user";
    }

    public void testFieldMappings() throws Exception {
        final String index = "logstash-20-12-2015";
        final String field = "foo";
        indexRandom(true, prepareIndex(index).setSource(field, "bar"));

        GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings()
            .addIndices("logstash-*")
            .setFields("*")
            .includeDefaults(true)
            .get();
        FieldMappingMetadata fieldMappingMetadata = response.fieldMappings(index, field);
        assertThat(fieldMappingMetadata, notNullValue());

        response = client().filterWithHeader(
            singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD))
        ).admin().indices().prepareGetFieldMappings().addIndices("logstash-*").setFields("*").includeDefaults(true).get();
        FieldMappingMetadata fieldMappingMetadata1 = response.fieldMappings(index, field);
        assertThat(fieldMappingMetadata1, notNullValue());
        assertThat(fieldMappingMetadata1.fullName(), equalTo(fieldMappingMetadata.fullName()));
    }

    public void testValidateQuery() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, prepareIndex(index).setSource(field, "bar"));

        ValidateQueryResponse response = indicesAdmin().prepareValidateQuery(index).setQuery(QueryBuilders.termQuery(field, "bar")).get();
        assertThat(response.isValid(), is(true));

        response = client().filterWithHeader(
            singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD))
        ).admin().indices().prepareValidateQuery(index).setQuery(QueryBuilders.termQuery(field, "bar")).get();
        assertThat(response.isValid(), is(true));
    }

    public void testSearchAndMSearch() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, prepareIndex(index).setSource(field, "bar"));

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), response -> {
            final long hits = response.getHits().getTotalHits().value();
            assertThat(hits, greaterThan(0L));
            assertResponse(
                client().filterWithHeader(
                    singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD))
                ).prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()),
                response2 -> assertEquals(response2.getHits().getTotalHits().value(), hits)
            );
            final long multiHits;
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()))
                .get();
            try {
                multiHits = multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value();
                assertThat(hits, greaterThan(0L));
            } finally {
                multiSearchResponse.decRef();
            }
            multiSearchResponse = client().filterWithHeader(
                singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD))
            ).prepareMultiSearch().add(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())).get();
            try {
                assertEquals(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value(), multiHits);
            } finally {
                multiSearchResponse.decRef();
            }
        });
    }

    public void testGetIndex() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "event";
        final String field = "foo";
        indexRandom(true, prepareIndex(index).setSource(field, "bar"));

        GetIndexResponse response = indicesAdmin().prepareGetIndex().setIndices(index).get();
        assertThat(response.getIndices(), arrayContaining(index));

        response = client().filterWithHeader(
            singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD))
        ).admin().indices().prepareGetIndex().setIndices(index).get();
        assertThat(response.getIndices(), arrayContaining(index));
    }

    public void testGetMappings() throws Exception {
        final String index = "logstash-20-12-2015";
        final String type = "_doc";
        final String field = "foo";
        indexRandom(true, prepareIndex(index).setSource(field, "bar"));

        GetMappingsResponse response = client().filterWithHeader(
            singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue("kibana_user", USERS_PASSWD))
        ).admin().indices().prepareGetMappings("logstash-*").get();
        Map<String, MappingMetadata> mappingsMap = response.getMappings();
        assertNotNull(mappingsMap);
        assertNotNull(mappingsMap.get(index));
        MappingMetadata mappingMetadata = mappingsMap.get(index);
        assertThat(mappingMetadata.getSourceAsMap(), hasKey("properties"));
        assertThat(mappingMetadata.getSourceAsMap().get("properties"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> propertiesMap = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        assertThat(propertiesMap, hasKey(field));
    }

    // TODO: When we have an XPackIntegTestCase, this should test that we can send MonitoringBulkActions

}
