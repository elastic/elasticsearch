/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that mappings containing supplementary Unicode characters (above U+FFFF) in field names
 * survive a rolling upgrade. Jackson's UTF8JsonGenerator encodes such characters as JSON surrogate
 * pair escapes (e.g. {@code \}{@code uD83C\}{@code uDFB5} for U+1F3B5). The parser must correctly
 * handle these when deserializing field names on any node version.
 * <p>
 * This is a regression guard for <a href="https://github.com/FasterXML/jackson-core/issues/1541">jackson-core#1541</a>.
 */
public class MappingSupplementaryCharacterRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    // U+1F3B5 MUSICAL NOTE
    private static final String FIELD_NOTE = "music_\uD83C\uDFB5_field";
    // U+1F60A SMILING FACE WITH SMILING EYES
    private static final String FIELD_SMILE = "smile_\uD83D\uDE0A_field";
    // U+1F389 PARTY POPPER
    private static final String FIELD_PARTY = "party_\uD83C\uDF89_field";

    private static final String INDEX_OLD = "supplementary_char_old";
    private static final String INDEX_MIXED = "supplementary_char_mixed";
    private static final String INDEX_UPGRADED = "supplementary_char_upgraded";

    public MappingSupplementaryCharacterRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testMappingWithSupplementaryCharacterFieldName() throws IOException {
        if (isOldCluster()) {
            // Old node creates index — all nodes must parse this mapping from cluster state
            createIndex(INDEX_OLD, FIELD_NOTE);
            indexDocument(INDEX_OLD, FIELD_NOTE, "1", "hello");
            assertDocumentExists(INDEX_OLD, FIELD_NOTE, "1", "hello");
        } else if (isMixedCluster()) {
            ensureGreen(INDEX_OLD);
            assertMappingContainsField(INDEX_OLD, FIELD_NOTE);
            assertDocumentExists(INDEX_OLD, FIELD_NOTE, "1", "hello");

            if (isFirstMixedCluster()) {
                // New node creates index while old nodes are still present — old nodes must parse this
                createIndex(INDEX_MIXED, FIELD_SMILE);
                indexDocument(INDEX_MIXED, FIELD_SMILE, "1", "mixed1");
                assertDocumentExists(INDEX_MIXED, FIELD_SMILE, "1", "mixed1");

                // Also force a mapping update on the old index so every node re-parses it
                addFieldToMapping(INDEX_OLD, FIELD_PARTY);
            } else {
                ensureGreen(INDEX_MIXED);
                assertMappingContainsField(INDEX_MIXED, FIELD_SMILE);
                assertMappingContainsField(INDEX_OLD, FIELD_PARTY);
                indexDocument(INDEX_OLD, FIELD_NOTE, "2", "mixed2");
                assertDocumentExists(INDEX_OLD, FIELD_NOTE, "2", "mixed2");
                indexDocument(INDEX_MIXED, FIELD_SMILE, "2", "mixed2");
                assertDocumentExists(INDEX_MIXED, FIELD_SMILE, "2", "mixed2");
            }
        } else {
            assert isUpgradedCluster();
            ensureGreen(INDEX_OLD);
            ensureGreen(INDEX_MIXED);

            // Verify everything from previous phases survived
            assertDocumentExists(INDEX_OLD, FIELD_NOTE, "1", "hello");
            assertDocumentExists(INDEX_OLD, FIELD_NOTE, "2", "mixed2");
            assertDocumentExists(INDEX_MIXED, FIELD_SMILE, "1", "mixed1");
            assertDocumentExists(INDEX_MIXED, FIELD_SMILE, "2", "mixed2");
            assertMappingContainsField(INDEX_OLD, FIELD_NOTE);
            assertMappingContainsField(INDEX_OLD, FIELD_PARTY);
            assertMappingContainsField(INDEX_MIXED, FIELD_SMILE);

            // Fully upgraded cluster creates its own index
            createIndex(INDEX_UPGRADED, FIELD_NOTE);
            indexDocument(INDEX_UPGRADED, FIELD_NOTE, "1", "upgraded");
            assertDocumentExists(INDEX_UPGRADED, FIELD_NOTE, "1", "upgraded");
        }
    }

    private void createIndex(String indexName, String fieldName) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        XContentBuilder body = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("settings")
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex.setJsonEntity(Strings.toString(body));
        assertThat(client().performRequest(createIndex).getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private void indexDocument(String indexName, String fieldName, String id, String value) throws IOException {
        Request indexRequest = new Request("PUT", "/" + indexName + "/_doc/" + id);
        indexRequest.addParameter("refresh", "true");
        XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field(fieldName, value).endObject();
        indexRequest.setJsonEntity(Strings.toString(doc));
        assertThat(client().performRequest(indexRequest).getStatusLine().getStatusCode(), equalTo(RestStatus.CREATED.getStatus()));
    }

    /**
     * Adds a new field with a supplementary character to an existing mapping, forcing a mapping
     * update that must be serialized and parsed by every node in the cluster.
     */
    private void addFieldToMapping(String indexName, String fieldName) throws IOException {
        Request putMapping = new Request("PUT", "/" + indexName + "/_mapping");
        XContentBuilder body = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        putMapping.setJsonEntity(Strings.toString(body));
        assertThat(client().performRequest(putMapping).getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @SuppressWarnings("unchecked")
    private void assertDocumentExists(String indexName, String fieldName, String id, String expectedValue) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + indexName + "/_doc/" + id));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> source = (Map<String, Object>) responseMap.get("_source");
        assertThat(source.get(fieldName), equalTo(expectedValue));
    }

    @SuppressWarnings("unchecked")
    private void assertMappingContainsField(String indexName, String fieldName) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> indexMapping = (Map<String, Object>) responseMap.get(indexName);
        Map<String, Object> mappings = (Map<String, Object>) indexMapping.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertNotNull("Mapping for [" + indexName + "] should contain field [" + fieldName + "]", properties.get(fieldName));
    }
}
