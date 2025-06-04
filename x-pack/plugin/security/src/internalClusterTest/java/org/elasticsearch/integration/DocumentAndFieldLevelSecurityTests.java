/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DocumentAndFieldLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() + Strings.format("""
            user1:%s
            user2:%s
            user3:%s
            user4:%s
            user5:%s
            """, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed);
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "role1:user1\n" + "role2:user1,user4\n" + "role3:user2,user4\n" + "role4:user3,user4,user5\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """

            role1:
              cluster: [ none ]
              indices:
                - names: '*'
                  privileges: [ none ]
            role2:
              cluster:
               - all
              indices:
                - names: '*'
                  privileges: [ ALL ]
                  field_security:
                     grant: [ field1, id ]
                  query: '{"term" : {"field1" : "value1"}}'
            role3:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ ALL ]
                  field_security:
                     grant: [ field2, id ]
                  query: '{"term" : {"field2" : "value2"}}'
            role4:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ ALL ]
                  field_security:
                     grant: [ field1, id ]
                  query: '{"term" : {"field2" : "value2"}}'
            """;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .build();
    }

    public void testSimpleQuery() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("id", "type=keyword", "field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("id", "1", "field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("id", "2", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                assertHitCount(response, 1);
                assertSearchHits(response, "1");
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(2));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("id").toString(), equalTo("1"));
            }
        );

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                assertHitCount(response, 1);
                assertSearchHits(response, "2");
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(2));
                assertThat(source.get("field2").toString(), equalTo("value2"));
                assertThat(source.get("id").toString(), equalTo("2"));
            }
        );

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 2);
                assertSearchHits(response, "1", "2");
                assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value1"));
                assertThat(response.getHits().getAt(1).getSourceAsMap().get("field2").toString(), equalTo("value2"));
            }
        );
    }

    public void testUpdatesAreRejected() {
        for (String indexName : List.of("<test-{2015.05.05||+1d}>", "test")) {
            assertAcked(
                indicesAdmin().prepareCreate(indexName)
                    .setMapping("id", "type=keyword", "field1", "type=text", "field2", "type=text")
                    .setSettings(indexSettings(1, 0))
            );
            prepareIndex(indexName).setId("1").setSource("id", "1", "field1", "value1").setRefreshPolicy(IMMEDIATE).get();

            ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class, () -> {
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareUpdate(indexName, "1")
                    .setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2")
                    .get();
            });
            assertThat(
                exception.getDetailedMessage(),
                containsString("Can't execute an update request if field or document level " + "security")
            );

            BulkResponse bulkResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareBulk().add(client().prepareUpdate(indexName, "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2")).get();
            assertThat(bulkResponse.getItems().length, is(1));
            assertThat(
                bulkResponse.getItems()[0].getFailureMessage(),
                containsString(
                    "Can't execute a bulk item request with update "
                        + "requests"
                        + " embedded if field or document level security is enabled"
                )
            );
        }
    }

    public void testDLSIsAppliedBeforeFLS() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field1", "value2", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(QueryBuilders.termQuery("field1", "value2")),
            response -> {
                assertHitCount(response, 1);
                assertSearchHits(response, "2");
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(1));
                assertThat(source.get("field1").toString(), equalTo("value2"));
            }
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(QueryBuilders.termQuery("field1", "value1")),
            0
        );
    }

    public void testQueryCache() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                .setMapping("id", "type=keyword", "field1", "type=text", "field2", "type=text")
        );
        prepareIndex("test").setId("1").setSource("id", "1", "field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("id", "2", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        // Both users have the same role query, but user3 has access to field2 and not field1, which should result in zero hits:
        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test"),
                response -> {
                    assertHitCount(response, 1);
                    assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                    Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                    assertThat(source.size(), equalTo(2));
                    assertThat(source.get("field1"), equalTo("value1"));
                    assertThat(source.get("id"), equalTo("1"));
                }
            );
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test"),
                response -> {
                    assertHitCount(response, 1);
                    assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
                    Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                    assertThat(source.size(), equalTo(2));
                    assertThat(source.get("field2"), equalTo("value2"));
                    assertThat(source.get("id"), equalTo("2"));
                }
            );

            // this is a bit weird the document level permission (all docs with field2:value2) don't match with the field level
            // permissions (field1),
            // this results in document 2 being returned but no fields are visible:
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareSearch("test"),
                response -> {
                    assertHitCount(response, 1);
                    assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
                    Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                    assertThat(source.size(), equalTo(1));
                    assertThat(source.get("id"), equalTo("2"));
                }
            );

            // user4 has all roles
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .prepareSearch("test")
                    .addSort("id", SortOrder.ASC),
                response -> {
                    assertHitCount(response, 2);
                    assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                    Map<String, Object> source0 = response.getHits().getAt(0).getSourceAsMap();
                    assertThat(source0.size(), equalTo(2));
                    assertThat(source0.get("field1"), equalTo("value1"));
                    assertThat(source0.get("id"), equalTo("1"));
                    assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                    Map<String, Object> source1 = response.getHits().getAt(1).getSourceAsMap();
                    assertThat(source1.size(), equalTo(2));
                    assertThat(source1.get("field2"), equalTo("value2"));
                    assertThat(source1.get("id"), equalTo("2"));
                }
            );
        }
    }

    public void testGetMappingsIsFiltered() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
            assertExpectedMetadataFields(getMappingsResponse.getMappings(), "field1");
        }

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
            assertExpectedMetadataFields(getMappingsResponse.getMappings(), "field2");
        }

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
            ).admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
            assertExpectedMetadataFields(getMappingsResponse.getMappings(), "field1");
        }

        {
            GetMappingsResponse getMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
            ).admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
            assertExpectedMetadataFields(getMappingsResponse.getMappings(), "field1", "field2");
        }
    }

    public void testGetIndexMappingsIsFiltered() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("test").get();
            assertExpectedMetadataFields(getIndexResponse.getMappings(), "field1");
        }
        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("test").get();
            assertExpectedMetadataFields(getIndexResponse.getMappings(), "field2");
        }
        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
            ).admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("test").get();
            assertExpectedMetadataFields(getIndexResponse.getMappings(), "field1");
        }
        {
            GetIndexResponse getIndexResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
            ).admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("test").get();
            assertExpectedMetadataFields(getIndexResponse.getMappings(), "field1", "field2");
        }
    }

    public void testGetFieldMappingsIsFiltered() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings = getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field1");
        }
        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings = getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field2");
        }
        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
            ).admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings = getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field1");
        }
        {
            GetFieldMappingsResponse getFieldMappingsResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
            ).admin().indices().prepareGetFieldMappings("test").setFields("*").get();

            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings = getFieldMappingsResponse.mappings();
            assertEquals(1, mappings.size());
            assertExpectedFields(mappings.get("test"), "field1", "field2");
        }
    }

    public void testFieldCapabilitiesIsFiltered() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field1");
        }
        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field2");
        }
        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
            ).fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field1");
        }
        {
            FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().fields("*").indices("test");
            FieldCapabilitiesResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
            ).fieldCaps(fieldCapabilitiesRequest).actionGet();
            assertExpectedFields(response, "field1", "field2");
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertExpectedMetadataFields(Map<String, MappingMetadata> mappings, String... fields) {
        Map<String, Object> sourceAsMap = mappings.get("test").getSourceAsMap();
        assertEquals(1, sourceAsMap.size());
        Map<String, Object> properties = (Map<String, Object>) sourceAsMap.get("properties");
        assertEquals(fields.length, properties.size());
        for (String field : fields) {
            assertNotNull(properties.get(field));
        }
    }

    private static void assertExpectedFields(FieldCapabilitiesResponse fieldCapabilitiesResponse, String... expectedFields) {
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>(fieldCapabilitiesResponse.get());
        for (String field : fieldCapabilitiesResponse.get().keySet()) {
            if (fieldCapabilitiesResponse.isMetadataField(field)) {
                assertNotNull(" expected field [" + field + "] not found", responseMap.remove(field));
            }
        }
        for (String field : expectedFields) {
            Map<String, FieldCapabilities> remove = responseMap.remove(field);
            assertNotNull(" expected field [" + field + "] not found", remove);
        }
        assertEquals("Some unexpected fields were returned: " + responseMap.keySet(), 0, responseMap.size());
    }

    private static void assertExpectedFields(Map<String, GetFieldMappingsResponse.FieldMappingMetadata> actual, String... expectedFields) {
        Map<String, GetFieldMappingsResponse.FieldMappingMetadata> fields = new HashMap<>(actual);
        for (String field : actual.keySet()) {
            // best effort to remove metadata fields
            if (field.startsWith("_")) {
                assertNotNull(" expected field [" + field + "] not found", fields.remove(field));
            }
        }
        for (String field : expectedFields) {
            GetFieldMappingsResponse.FieldMappingMetadata fieldMappingMetadata = fields.remove(field);
            assertNotNull("expected field [" + field + "] not found", fieldMappingMetadata);
        }
        assertEquals("Some unexpected fields were returned: " + fields.keySet(), 0, fields.size());
    }
}
