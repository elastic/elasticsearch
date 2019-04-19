/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class DocumentAndFieldLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));

        return super.configUsers() +
            "user1:" + usersPasswdHashed + "\n" +
            "user2:" + usersPasswdHashed + "\n" +
            "user3:" + usersPasswdHashed + "\n" +
            "user4:" + usersPasswdHashed + "\n" +
            "user5:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1\n" +
                "role2:user1,user4\n" +
                "role3:user2,user4\n" +
                "role4:user3,user4,user5\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: [ none ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ none ]\n" +
                "role2:\n" +
                "  cluster:\n" +
                "   - all\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      field_security:\n" +
                "         grant: [ field1 ]\n" +
                "      query: '{\"term\" : {\"field1\" : \"value1\"}}'\n" +
                "role3:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      field_security:\n" +
                "         grant: [ field2 ]\n" +
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'\n" +
                "role4:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      field_security:\n" +
                "         grant: [ field1 ]\n" +
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'\n";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
                .build();
    }

    public void testSimpleQuery() {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "2");
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field2").toString(), equalTo("value2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("_id", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertSearchHits(response, "1", "2");
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(1).getSourceAsMap().get("field2").toString(), equalTo("value2"));
    }

    public void testDLSIsAppliedBeforeFLS() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test").setQuery(QueryBuilders.termQuery("field1", "value2"))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "2");
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value2"));

        response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test").setQuery(QueryBuilders.termQuery("field1", "value1"))
                .get();
        assertHitCount(response, 0);
    }

    public void testQueryCache() {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                        .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // Both users have the same role query, but user3 has access to field2 and not field1, which should result in zero hits:
        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            SearchResponse response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value1"));
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap().get("field2"), equalTo("value2"));

            // this is a bit weird the document level permission (all docs with field2:value2) don't match with the field level
            // permissions (field1),
            // this results in document 2 being returned but no fields are visible:
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareSearch("test")
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(0));

            // user4 has all roles
            response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .prepareSearch("test")
                    .addSort("_id", SortOrder.ASC)
                    .get();
            assertHitCount(response, 2);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value1"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(1).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(1).getSourceAsMap().get("field2"), equalTo("value2"));
        }
    }

    public void testGetMappingsIsFiltered() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .admin().indices().prepareGetMappings("test").get();
            assertExpectedFields(getMappingsResponse.getMappings(), "field1");
        }

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .admin().indices().prepareGetMappings("test").get();
            assertExpectedFields(getMappingsResponse.getMappings(), "field2");
        }

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .admin().indices().prepareGetMappings("test").get();
            assertExpectedFields(getMappingsResponse.getMappings(), "field1");
        }

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .admin().indices().prepareGetMappings("test").get();
            assertExpectedFields(getMappingsResponse.getMappings(), "field1", "field2");
        }
    }

    public void testGetIndexMappingsIsFiltered() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .admin().indices().prepareGetIndex().setIndices("test").get();
            assertExpectedFields(getIndexResponse.getMappings(), "field1");
        }
        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .admin().indices().prepareGetIndex().setIndices("test").get();
            assertExpectedFields(getIndexResponse.getMappings(), "field2");
        }
        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .admin().indices().prepareGetIndex().setIndices("test").get();
            assertExpectedFields(getIndexResponse.getMappings(), "field1");
        }
        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .admin().indices().prepareGetIndex().setIndices("test").get();
            assertExpectedFields(getIndexResponse.getMappings(), "field1", "field2");
        }
    }

    public void testGetFieldMappingsIsFiltered() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings =
                    getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field1");
        }
        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings =
                    getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field2");
        }
        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings =
                    getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field1");
        }
        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings =
                    getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field1", "field2");
        }
    }

    public void testFieldCapabilitiesIsFiltered() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field1");
        }
        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field2");
        }
        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field1");
        }
        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field1", "field2");
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertExpectedFields(ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings,
                                             String... fields) {
        Map<String, Object> sourceAsMap = mappings.get("test").get("type1").getSourceAsMap();
        assertEquals(1, sourceAsMap.size());
        Map<String, Object> properties = (Map<String, Object>)sourceAsMap.get("properties");
        assertEquals(fields.length, properties.size());
        for (String field : fields) {
            assertNotNull(properties.get(field));
        }
    }

    private static void assertExpectedFields(FieldCapabilitiesResponse fieldCapabilitiesResponse, String... expectedFields) {
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>(fieldCapabilitiesResponse.get());
        Set<String> builtInMetaDataFields = IndicesModule.getBuiltInMetaDataFields();
        for (String field : builtInMetaDataFields) {
            Map<String, FieldCapabilities> remove = responseMap.remove(field);
            assertNotNull(" expected field [" + field + "] not found", remove);
        }
        for (String field : expectedFields) {
            Map<String, FieldCapabilities> remove = responseMap.remove(field);
            assertNotNull(" expected field [" + field + "] not found", remove);
        }
        assertEquals("Some unexpected fields were returned: " + responseMap.keySet(), 0, responseMap.size());
    }

    private static void assertExpectedFields(Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>> mappings,
                                            String... expectedFields) {
        assertEquals(1, mappings.size());
        Map<String, GetFieldMappingsResponse.FieldMappingMetaData> fields = new HashMap<>(mappings.get("type1"));
        Set<String> builtInMetaDataFields = IndicesModule.getBuiltInMetaDataFields();
        for (String field : builtInMetaDataFields) {
            GetFieldMappingsResponse.FieldMappingMetaData fieldMappingMetaData = fields.remove(field);
            assertNotNull(" expected field [" + field + "] not found", fieldMappingMetaData);
        }
        for (String field : expectedFields) {
            GetFieldMappingsResponse.FieldMappingMetaData fieldMappingMetaData = fields.remove(field);
            assertNotNull("expected field [" + field + "] not found", fieldMappingMetaData);
        }
        assertEquals("Some unexpected fields were returned: " + fields.keySet(), 0, fields.size());
    }
}
