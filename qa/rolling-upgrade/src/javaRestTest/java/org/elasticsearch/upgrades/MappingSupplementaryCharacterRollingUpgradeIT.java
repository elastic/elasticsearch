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

    private static final String INDEX_NAME = "supplementary_char_mapping";
    // U+1F3B5 MUSICAL NOTE encoded as a Java surrogate pair
    private static final String SUPPLEMENTARY_FIELD = "music_\uD83C\uDFB5_field";

    public MappingSupplementaryCharacterRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testMappingWithSupplementaryCharacterFieldName() throws IOException {
        if (isOldCluster()) {
            createIndexWithSupplementaryCharField();
            indexDocument("1", "hello");
            assertDocumentExists("1", "hello");
        } else if (isMixedCluster()) {
            ensureGreen(INDEX_NAME);
            assertMappingContainsField();
            assertDocumentExists("1", "hello");
            // Index a document while old and new nodes coexist to exercise cross-version cluster state propagation
            if (isFirstMixedCluster()) {
                indexDocument("2", "mixed1");
                assertDocumentExists("2", "mixed1");
                // Add a new field to force a mapping update that must be parsed by all nodes
                addFieldToMapping();
            } else {
                indexDocument("3", "mixed2");
                assertDocumentExists("3", "mixed2");
            }
        } else {
            assert isUpgradedCluster();
            ensureGreen(INDEX_NAME);
            assertDocumentExists("1", "hello");
            assertDocumentExists("2", "mixed1");
            assertDocumentExists("3", "mixed2");
            indexDocument("4", "upgraded");
            assertDocumentExists("4", "upgraded");
            assertMappingContainsField();
        }
    }

    private void createIndexWithSupplementaryCharField() throws IOException {
        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        XContentBuilder body = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("settings")
            .field("number_of_shards", 1)
            .field("number_of_replicas", 1)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject(SUPPLEMENTARY_FIELD)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex.setJsonEntity(Strings.toString(body));
        assertThat(client().performRequest(createIndex).getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private void indexDocument(String id, String value) throws IOException {
        Request indexRequest = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + id);
        indexRequest.addParameter("refresh", "true");
        XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field(SUPPLEMENTARY_FIELD, value)
            .endObject();
        indexRequest.setJsonEntity(Strings.toString(doc));
        assertThat(client().performRequest(indexRequest).getStatusLine().getStatusCode(), equalTo(RestStatus.CREATED.getStatus()));
    }

    /**
     * Adds a new field with a supplementary character to the existing mapping, forcing a mapping
     * update that must be serialized and parsed by every node in the cluster, including old nodes
     * running a different Jackson version.
     */
    private void addFieldToMapping() throws IOException {
        Request putMapping = new Request("PUT", "/" + INDEX_NAME + "/_mapping");
        // U+1F60A SMILING FACE WITH SMILING EYES
        String newField = "smile_\uD83D\uDE0A_field";
        XContentBuilder body = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("properties")
            .startObject(newField)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        putMapping.setJsonEntity(Strings.toString(body));
        assertThat(client().performRequest(putMapping).getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @SuppressWarnings("unchecked")
    private void assertDocumentExists(String id, String expectedValue) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_doc/" + id));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> source = (Map<String, Object>) responseMap.get("_source");
        assertThat(source.get(SUPPLEMENTARY_FIELD), equalTo(expectedValue));
    }

    @SuppressWarnings("unchecked")
    private void assertMappingContainsField() throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_mapping"));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> indexMapping = (Map<String, Object>) responseMap.get(INDEX_NAME);
        Map<String, Object> mappings = (Map<String, Object>) indexMapping.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertNotNull("Mapping should contain the supplementary character field", properties.get(SUPPLEMENTARY_FIELD));
    }
}
