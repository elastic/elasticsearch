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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that mappings containing supplementary Unicode characters (above U+FFFF) in field names
 * survive a full cluster restart / upgrade. Jackson's UTF8JsonGenerator encodes such characters as
 * JSON surrogate pair escapes (e.g. {@code 🎵} for U+1F3B5). The parser must correctly
 * handle these when deserializing cluster state on the upgraded node.
 * <p>
 * Regression guard for <a href="https://github.com/FasterXML/jackson-core/issues/1541">jackson-core#1541</a>,
 * which affected Jackson 2.21.0 and 2.21.1 and was fixed in 2.21.2.
 */
public class MappingSupplementaryCharacterFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    // U+1F3B5 MUSICAL NOTE
    private static final String FIELD_NAME = "music_🎵_field";

    private static final String INDEX_NAME = "supplementary_char_restart";

    @ClassRule
    public static final ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        Version oldVersion = Version.fromString(OLD_CLUSTER_VERSION);
        var c = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
            .setting("xpack.security.enabled", "false");

        if (oldVersion.before(Version.fromString("8.18.0"))) {
            c.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            c.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }
        return c.build();
    }

    public MappingSupplementaryCharacterFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testMappingWithSupplementaryCharacterFieldName() throws IOException {
        if (isRunningAgainstOldCluster()) {
            createIndex();
            indexDocument("1", "hello");
            assertDocumentValue("1", "hello");
        } else {
            ensureGreen(INDEX_NAME);
            assertMappingContainsField();
            assertDocumentValue("1", "hello");
            indexDocument("2", "world");
            assertDocumentValue("2", "world");
        }
    }

    private void createIndex() throws IOException {
        Request request = new Request("PUT", "/" + INDEX_NAME);
        XContentBuilder body = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("settings")
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject(FIELD_NAME)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        request.setJsonEntity(Strings.toString(body));
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private void indexDocument(String id, String value) throws IOException {
        Request request = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + id);
        request.addParameter("refresh", "true");
        XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field(FIELD_NAME, value).endObject();
        request.setJsonEntity(Strings.toString(doc));
        int status = client().performRequest(request).getStatusLine().getStatusCode();
        assertThat(status, equalTo(RestStatus.CREATED.getStatus()));
    }

    @SuppressWarnings("unchecked")
    private void assertDocumentValue(String id, String expectedValue) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_doc/" + id));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> source = (Map<String, Object>) responseMap.get("_source");
        assertThat(source.get(FIELD_NAME), equalTo(expectedValue));
    }

    @SuppressWarnings("unchecked")
    private void assertMappingContainsField() throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_mapping"));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> indexMapping = (Map<String, Object>) responseMap.get(INDEX_NAME);
        Map<String, Object> mappings = (Map<String, Object>) indexMapping.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertThat("mapping should contain field [" + FIELD_NAME + "]", properties.get(FIELD_NAME), notNullValue());
    }
}
