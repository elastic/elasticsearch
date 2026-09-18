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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that supplementary Unicode characters (above U+FFFF) in field <em>values</em> survive a
 * rolling upgrade, and that the byte-level encoding in responses changes as expected when the cluster
 * moves to a version with {@link com.fasterxml.jackson.core.JsonGenerator.Feature#COMBINE_UNICODE_SURROGATES_IN_UTF8}
 * enabled.
 *
 * <p>Two independent behaviors are demonstrated:
 * <ol>
 *   <li><strong>GET {@code _source} is verbatim.</strong> Whatever bytes were stored during indexing
 *       are returned as-is. A document indexed with the old surrogate-escape encoding
 *       ({@code 🎵}) will continue to return those bytes from GET after the upgrade.
 *       New Jackson can parse both the old and new forms, so semantic values are always correct.</li>
 *   <li><strong>Re-serialized responses use the node's current Jackson.</strong> Paths such as
 *       aggregation results build Java {@link String} objects from Lucene's stored UTF-8 bytes and
 *       serialize them afresh. On old nodes this produces surrogate-escape JSON; on upgraded nodes
 *       it produces proper 4-byte UTF-8.</li>
 * </ol>
 */
public class SupplementaryCharacterValueEncodingRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA) // Remove once 9.6.0 is the minimum supported upgrade version
    @BeforeClass
    public static void skipIfNotRelevant() {
        assumeTrue(
            "Only meaningful when upgrading from before COMBINE_UNICODE_SURROGATES_IN_UTF8 (pre-9.6.0)",
            Version.tryParse(getOldClusterVersion()).map(v -> v.before(Version.fromString("9.6.0"))).orElse(false)
        );
    }

    // U+1F3B5 MUSICAL NOTE as a Java surrogate pair
    private static final String EMOJI = "🎵";

    // Old encoding: each surrogate half written as a JSON Unicode escape, derived from EMOJI's chars
    private static final String EMOJI_SURROGATE_ESCAPES = asJsonUnicodeEscapes(EMOJI);

    // New encoding: proper UTF-8 bytes for EMOJI, viewed via ISO-8859-1 for byte-level comparison
    private static final String EMOJI_UTF8_AS_LATIN1 = asLatin1(EMOJI.getBytes(StandardCharsets.UTF_8));

    private static final String INDEX = "emoji_value_encoding";

    public SupplementaryCharacterValueEncodingRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testEmojiInFieldValue() throws IOException {
        byte[] bytes;
        if (isOldCluster()) {
            createIndex();

            // Doc 1: raw JSON with surrogate-escape encoding, as old Jackson would have produced
            indexRaw("1", "{\"content\":\"" + EMOJI_SURROGATE_ESCAPES + "\"}");

            // GET _source is verbatim: old bytes are returned as stored
            bytes = getRawGetBytes("1");
            assertThat(asLatin1(bytes), containsString(EMOJI_SURROGATE_ESCAPES));
            assertThat(asLatin1(bytes), not(containsString(EMOJI_UTF8_AS_LATIN1)));
            assertDocValue("1", EMOJI);

            // Terms agg re-serializes through Jackson: old cluster uses surrogate escapes
            bytes = getRawAggBytes();
            assertThat(asLatin1(bytes), containsString(EMOJI_SURROGATE_ESCAPES));
            assertThat(asLatin1(bytes), not(containsString(EMOJI_UTF8_AS_LATIN1)));
            assertAggBucketKey(EMOJI);
        } else if (isMixedCluster()) {
            ensureGreen(INDEX);
            // Semantics must hold regardless of which node (old or new) handles the request
            assertDocValue("1", EMOJI);
        } else {
            assertTrue(isUpgradedCluster());
            ensureGreen(INDEX);

            // Doc 1: GET _source still returns the verbatim old bytes — backward compat
            bytes = getRawGetBytes("1");
            assertThat(asLatin1(bytes), containsString(EMOJI_SURROGATE_ESCAPES));
            assertThat(asLatin1(bytes), not(containsString(EMOJI_UTF8_AS_LATIN1)));
            assertDocValue("1", EMOJI);

            // Terms agg now re-serializes with new Jackson: proper 4-byte UTF-8
            bytes = getRawAggBytes();
            assertThat(asLatin1(bytes), containsString(EMOJI_UTF8_AS_LATIN1));
            assertThat(asLatin1(bytes), not(containsString(EMOJI_SURROGATE_ESCAPES)));
            assertAggBucketKey(EMOJI);

            // Doc 2: indexed on upgraded cluster — XContentBuilder uses new Jackson, stores 4-byte UTF-8
            indexXContent("2");
            bytes = getRawGetBytes("2");
            assertThat(asLatin1(bytes), containsString(EMOJI_UTF8_AS_LATIN1));
            assertThat(asLatin1(bytes), not(containsString(EMOJI_SURROGATE_ESCAPES)));
            assertDocValue("2", EMOJI);
        }
    }

    private void createIndex() throws IOException {
        Request request = new Request("PUT", "/" + INDEX);
        request.setJsonEntity("""
            {
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
              },
              "mappings": {
                "properties": {
                  "content": {
                    "type": "keyword"
                  }
                }
              }
            }
            """);
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    /** Indexes a document using a pre-formed JSON string, so the exact bytes are under test control. */
    private void indexRaw(String id, String jsonBody) throws IOException {
        Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(jsonBody);
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(RestStatus.CREATED.getStatus()));
    }

    /** Indexes a document via {@link XContentBuilder}, which uses the current node's Jackson encoding. */
    private void indexXContent(String id) throws IOException {
        Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field("content", EMOJI).endObject();
        request.setJsonEntity(Strings.toString(doc));
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(RestStatus.CREATED.getStatus()));
    }

    /**
     * Returns the raw bytes of a GET _doc response. GET embeds {@code _source} verbatim, so
     * the byte-level encoding reflects exactly what was stored at index time.
     */
    private byte[] getRawGetBytes(String id) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_doc/" + id));
        return EntityUtils.toByteArray(response.getEntity());
    }

    @SuppressWarnings("unchecked")
    private void assertDocValue(String id, String expected) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_doc/" + id));
        Map<String, Object> map = entityAsMap(response);
        Map<String, Object> source = (Map<String, Object>) map.get("_source");
        assertThat(source.get("content"), equalTo(expected));
    }

    private Response termsAgg() throws IOException {
        Request request = new Request("GET", "/" + INDEX + "/_search");
        request.setJsonEntity("{\"size\":0,\"aggs\":{\"values\":{\"terms\":{\"field\":\"content\"}}}}");
        return client().performRequest(request);
    }

    /**
     * Returns the raw bytes of a terms aggregation response. Aggregation results are built
     * from Lucene's stored term bytes and re-serialized through Jackson, so the encoding
     * reflects the serving node's Jackson version, not the indexing node's.
     */
    private byte[] getRawAggBytes() throws IOException {
        return EntityUtils.toByteArray(termsAgg().getEntity());
    }

    @SuppressWarnings("unchecked")
    private void assertAggBucketKey(String expected) throws IOException {
        Map<String, Object> map = entityAsMap(termsAgg());
        Map<String, Object> aggs = (Map<String, Object>) map.get("aggregations");
        Map<String, Object> values = (Map<String, Object>) aggs.get("values");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) values.get("buckets");
        assertThat(buckets.getFirst().get("key"), equalTo(expected));
    }

    /** Formats each {@code char} in {@code s} as a JSON Unicode escape sequence. */
    private static String asJsonUnicodeEscapes(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            sb.append(String.format(Locale.ROOT, "\\u%04X", (int) c));
        }
        return sb.toString();
    }

    private static String asLatin1(byte[] bytes) {
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}
