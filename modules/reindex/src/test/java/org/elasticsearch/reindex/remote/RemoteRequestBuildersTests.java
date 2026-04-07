/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.reindex.remote.RemoteRequestBuilders.clearScroll;
import static org.elasticsearch.reindex.remote.RemoteRequestBuilders.closePit;
import static org.elasticsearch.reindex.remote.RemoteRequestBuilders.initialSearch;
import static org.elasticsearch.reindex.remote.RemoteRequestBuilders.openPit;
import static org.elasticsearch.reindex.remote.RemoteRequestBuilders.pitSearch;
import static org.elasticsearch.reindex.remote.RemoteRequestBuilders.scroll;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for {@link RemoteRequestBuilders} which builds requests for remote version of
 * Elasticsearch. Note that unlike most of the rest of Elasticsearch this file needs to
 * be compatible with very old versions of Elasticsearch. Thus is often uses identifiers
 * for versions like {@code 2000099} for {@code 2.0.0-alpha1}. Do not drop support for
 * features from this file just because the version constants have been removed.
 */
public class RemoteRequestBuildersTests extends ESTestCase {
    public void testIntialSearchPath() {
        Version remoteVersion = Version.fromId(between(0, Version.CURRENT.id));
        BytesReference query = new BytesArray("{}");

        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        assertEquals("/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("a");
        assertEquals("/a/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("a", "b");
        assertEquals("/a,b/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("cat,");
        assertEquals("/cat%2C/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("cat/");
        assertEquals("/cat%2F/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("cat/", "dog");
        assertEquals("/cat%2F,dog/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        // test a specific date math + all characters that need escaping.
        searchRequest.indices("<cat{now/d}>", "<>/{}|+:,");
        assertEquals(
            "/%3Ccat%7Bnow%2Fd%7D%3E,%3C%3E%2F%7B%7D%7C%2B%3A%2C/_search",
            initialSearch(searchRequest, query, remoteVersion).getEndpoint()
        );

        // re-escape already escaped (no special handling).
        searchRequest.indices("%2f", "%3a");
        assertEquals("/%252f,%253a/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("%2fcat,");
        assertEquals("/%252fcat%2C/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
        searchRequest.indices("%3ccat/");
        assertEquals("/%253ccat%2F/_search", initialSearch(searchRequest, query, remoteVersion).getEndpoint());
    }

    public void testInitialSearchParamsSort() {
        BytesReference query = new BytesArray("{}");
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());

        // Test sort:_doc for versions that support it.
        Version remoteVersion = Version.fromId(between(2010099, Version.CURRENT.id));
        searchRequest.source().sort("_doc");
        assertThat(initialSearch(searchRequest, query, remoteVersion).getParameters(), hasEntry("sort", "_doc:asc"));

        // Test search_type scan for versions that don't support sort:_doc.
        remoteVersion = Version.fromId(between(0, 2010099 - 1));
        assertThat(initialSearch(searchRequest, query, remoteVersion).getParameters(), hasEntry("search_type", "scan"));

        // Test sorting by some field. Version doesn't matter.
        remoteVersion = Version.fromId(between(0, Version.CURRENT.id));
        searchRequest.source().sorts().clear();
        searchRequest.source().sort("foo");
        assertThat(initialSearch(searchRequest, query, remoteVersion).getParameters(), hasEntry("sort", "foo:asc"));
    }

    public void testInitialSearchParamsFields() {
        BytesReference query = new BytesArray("{}");
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());

        // Test request without any fields
        Version remoteVersion = Version.fromId(between(2000099, Version.CURRENT.id));
        assertThat(
            initialSearch(searchRequest, query, remoteVersion).getParameters(),
            not(either(hasKey("stored_fields")).or(hasKey("fields")))
        );

        // Test stored_fields for versions that support it
        searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        searchRequest.source().storedField("_source").storedField("_id");
        // V_5_0_0 (final) => current
        int minStoredFieldsVersion = 5000099;
        remoteVersion = Version.fromId(randomBoolean() ? minStoredFieldsVersion : between(minStoredFieldsVersion, Version.CURRENT.id));
        assertThat(initialSearch(searchRequest, query, remoteVersion).getParameters(), hasEntry("stored_fields", "_source,_id"));

        // Test fields for versions that support it
        searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        searchRequest.source().storedField("_source").storedField("_id");
        // V_2_0_0 => V_5_0_0_alpha3
        remoteVersion = Version.fromId(randomBoolean() ? minStoredFieldsVersion - 1 : between(2000099, minStoredFieldsVersion - 1));
        assertThat(initialSearch(searchRequest, query, remoteVersion).getParameters(), hasEntry("fields", "_source,_id"));

        // Test extra fields for versions that need it
        searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        searchRequest.source().storedField("_source").storedField("_id");
        remoteVersion = Version.fromId(between(0, 2000099 - 1));
        assertThat(
            initialSearch(searchRequest, query, remoteVersion).getParameters(),
            hasEntry("fields", "_source,_id,_parent,_routing,_ttl")
        );

        // But only versions before 1.0 force _source to be in the list
        searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        searchRequest.source().storedField("_id");
        remoteVersion = Version.fromId(between(1000099, 2000099 - 1));
        assertThat(initialSearch(searchRequest, query, remoteVersion).getParameters(), hasEntry("fields", "_id,_parent,_routing,_ttl"));
    }

    public void testInitialSearchParamsMisc() {
        BytesReference query = new BytesArray("{}");
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        Version remoteVersion = Version.fromId(between(0, Version.CURRENT.id));

        TimeValue scroll = null;
        if (randomBoolean()) {
            scroll = randomPositiveTimeValue();
            searchRequest.scroll(scroll);
        }
        int size = between(0, Integer.MAX_VALUE);
        searchRequest.source().size(size);
        Boolean fetchVersion = null;
        if (randomBoolean()) {
            fetchVersion = randomBoolean();
            searchRequest.source().version(fetchVersion);
        }

        Map<String, String> params = initialSearch(searchRequest, query, remoteVersion).getParameters();

        if (scroll == null) {
            assertThat(params, not(hasKey("scroll")));
        } else {
            assertScroll(remoteVersion, params, scroll);
        }
        assertThat(params, hasEntry("size", Integer.toString(size)));
        if (fetchVersion != null) {
            assertThat(params, fetchVersion ? hasEntry("version", Boolean.TRUE.toString()) : hasEntry("version", Boolean.FALSE.toString()));
        } else {
            assertThat(params, hasEntry("version", Boolean.FALSE.toString()));
        }
    }

    public void testInitialSearchDisallowPartialResults() {
        final String allowPartialParamName = "allow_partial_search_results";
        final int v6_3 = 6030099;

        BytesReference query = new BytesArray("{}");
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());

        Version disallowVersion = Version.fromId(between(v6_3, Version.CURRENT.id));
        Map<String, String> params = initialSearch(searchRequest, query, disallowVersion).getParameters();
        assertEquals("false", params.get(allowPartialParamName));

        Version allowVersion = Version.fromId(between(0, v6_3 - 1));
        params = initialSearch(searchRequest, query, allowVersion).getParameters();
        assertThat(params.keySet(), not(contains(allowPartialParamName)));
    }

    private void assertScroll(Version remoteVersion, Map<String, String> params, TimeValue requested) {
        // V_5_0_0
        if (remoteVersion.before(Version.fromId(5000099))) {
            // Versions of Elasticsearch prior to 5.0 can't parse nanos or micros in TimeValue.
            assertThat(params.get("scroll"), not(either(endsWith("nanos")).or(endsWith("micros"))));
            if (requested.getStringRep().endsWith("nanos") || requested.getStringRep().endsWith("micros")) {
                long millis = (long) Math.ceil(requested.millisFrac());
                assertEquals(TimeValue.parseTimeValue(params.get("scroll"), "scroll"), timeValueMillis(millis));
                return;
            }
        }
        assertEquals(requested, TimeValue.parseTimeValue(params.get("scroll"), "scroll"));
    }

    public void testInitialSearchEntity() throws IOException {
        Version remoteVersion = Version.fromId(between(0, Version.CURRENT.id));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        // always set by AbstractAsyncBulkByScrollAction#prepareSearchRequest
        searchRequest.source().excludeVectors(false);
        String query = "{\"match_all\":{}}";
        HttpEntity entity = initialSearch(searchRequest, new BytesArray(query), remoteVersion).getEntity();
        assertEquals(ContentType.APPLICATION_JSON.toString(), entity.getContentType().getValue());
        if (remoteVersion.onOrAfter(Version.fromId(1000099))) {
            if (remoteVersion.onOrAfter(Version.V_9_1_0)) {
                // vectors are automatically included on recent versions
                assertEquals(XContentHelper.stripWhitespace(Strings.format("""
                    {
                      "query": %s,
                      "_source": {
                        "exclude_vectors":false,
                        "includes": [],
                        "excludes": []
                      }
                    }""", query)), Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8)));
            } else {
                assertEquals(
                    "{\"query\":" + query + ",\"_source\":true}",
                    Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))
                );
            }
        } else {
            assertEquals(
                "{\"query\":" + query + "}",
                Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))
            );
        }

        // Source filtering is included if set up
        searchRequest.source().fetchSource(new String[] { "in1", "in2" }, new String[] { "out" });
        entity = initialSearch(searchRequest, new BytesArray(query), remoteVersion).getEntity();
        assertEquals(ContentType.APPLICATION_JSON.toString(), entity.getContentType().getValue());
        if (remoteVersion.onOrAfter(Version.V_9_1_0)) {
            // vectors are automatically included on recent versions
            assertEquals(XContentHelper.stripWhitespace(Strings.format("""
                {
                  "query": %s,
                  "_source": {
                    "exclude_vectors":false,
                    "includes": [ "in1", "in2" ],
                    "excludes": [ "out" ]
                  }
                }""", query)), Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8)));
        } else {
            assertEquals(XContentHelper.stripWhitespace(Strings.format("""
                {
                  "query": %s,
                  "_source": {
                    "includes": [ "in1", "in2" ],
                    "excludes": [ "out" ]
                  }
                }""", query)), Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8)));
        }

        // Invalid XContent fails
        RuntimeException e = expectThrows(
            RuntimeException.class,
            () -> initialSearch(searchRequest, new BytesArray("{}, \"trailing\": {}"), remoteVersion)
        );
        assertThat(e.getCause().getMessage(), containsString("Unexpected character (',' (code 44))"));
        e = expectThrows(RuntimeException.class, () -> initialSearch(searchRequest, new BytesArray("{"), remoteVersion));
        assertThat(e.getCause().getMessage(), containsString("Unexpected end-of-input"));
    }

    public void testInitialSearchProjectRouting() throws IOException {
        Version remoteVersion = Version.fromId(between(0, Version.CURRENT.id));
        String query = "{\"match_all\":{}}";
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        String projectRouting = "_alias:linked";
        searchRequest.setProjectRouting(projectRouting);
        String body = Streams.copyToString(
            new InputStreamReader(
                initialSearch(searchRequest, new BytesArray(query), remoteVersion).getEntity().getContent(),
                StandardCharsets.UTF_8
            )
        );
        assertThat(body, containsString("\"project_routing\":\"" + projectRouting + "\""));
    }

    public void testScrollParams() {
        String scroll = randomAlphaOfLength(30);
        Version remoteVersion = Version.fromId(between(0, Version.CURRENT.id));
        TimeValue keepAlive = randomPositiveTimeValue();
        assertScroll(remoteVersion, scroll(scroll, keepAlive, remoteVersion).getParameters(), keepAlive);
    }

    public void testScrollEntity() throws IOException {
        String scroll = randomAlphaOfLength(30);
        HttpEntity entity = scroll(scroll, timeValueMillis(between(1, 1000)), Version.fromString("5.0.0")).getEntity();
        assertEquals(ContentType.APPLICATION_JSON.toString(), entity.getContentType().getValue());
        assertThat(
            Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8)),
            containsString("\"" + scroll + "\"")
        );

        // Test with version < 2.0.0
        entity = scroll(scroll, timeValueMillis(between(1, 1000)), Version.fromId(1070499)).getEntity();
        assertEquals(ContentType.TEXT_PLAIN.toString(), entity.getContentType().getValue());
        assertEquals(scroll, Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8)));
    }

    public void testClearScroll() throws IOException {
        String scroll = randomAlphaOfLength(30);
        Request request = clearScroll(scroll, Version.fromString("5.0.0"));
        assertEquals(ContentType.APPLICATION_JSON.toString(), request.getEntity().getContentType().getValue());
        assertThat(
            Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8)),
            containsString("\"" + scroll + "\"")
        );
        assertThat(request.getParameters().keySet(), empty());

        // Test with version < 2.0.0
        request = clearScroll(scroll, Version.fromId(1070499));
        assertEquals(ContentType.TEXT_PLAIN.toString(), request.getEntity().getContentType().getValue());
        assertEquals(scroll, Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8)));
        assertThat(request.getParameters().keySet(), empty());
    }

    /**
     * Verifies that openPit builds a POST request to the correct path for a single index.
     */
    public void testOpenPitSingleIndex() {
        String index = randomAlphaOfLength(between(1, 20));
        TimeValue keepAlive = randomPositiveTimeValue();
        Request request = openPit(new String[] { index }, keepAlive, null);
        assertEquals("POST", request.getMethod());
        assertEquals("/" + index + "/_pit", request.getEndpoint());
        assertThat(request.getParameters(), hasEntry("keep_alive", keepAlive.getStringRep()));
        assertThat(request.getParameters(), hasEntry("allow_partial_search_results", "false"));
    }

    /**
     * Verifies that openPit builds a POST request to the correct path for multiple indices.
     */
    public void testOpenPitMultipleIndices() {
        int numIndices = between(2, 10);
        String[] indices = new String[numIndices];
        StringBuilder expectedPath = new StringBuilder("/");
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLength(between(1, 10));
            if (i > 0) {
                expectedPath.append(",");
            }
            expectedPath.append(indices[i]);
        }
        expectedPath.append("/_pit");
        TimeValue keepAlive = randomPositiveTimeValue();
        Request request = openPit(indices, keepAlive, null);
        assertEquals("POST", request.getMethod());
        assertEquals(expectedPath.toString(), request.getEndpoint());
        assertThat(request.getParameters(), hasEntry("keep_alive", keepAlive.getStringRep()));
        assertThat(request.getParameters(), hasEntry("allow_partial_search_results", "false"));
    }

    /**
     * Verifies that openPit uses /_pit when indices are null or empty, matching addIndices behavior.
     */
    public void testOpenPitNullOrEmptyIndices() {
        TimeValue keepAlive = randomPositiveTimeValue();
        Request nullRequest = openPit(null, keepAlive, null);
        assertEquals("POST", nullRequest.getMethod());
        assertEquals("/_pit", nullRequest.getEndpoint());
        assertThat(nullRequest.getParameters(), hasEntry("keep_alive", keepAlive.getStringRep()));
        assertThat(nullRequest.getParameters(), hasEntry("allow_partial_search_results", "false"));

        Request emptyRequest = openPit(new String[] {}, keepAlive, null);
        assertEquals("POST", emptyRequest.getMethod());
        assertEquals("/_pit", emptyRequest.getEndpoint());
        assertThat(emptyRequest.getParameters(), hasEntry("keep_alive", keepAlive.getStringRep()));
        assertThat(emptyRequest.getParameters(), hasEntry("allow_partial_search_results", "false"));
    }

    /**
     * Verifies that openPit URL-encodes index names containing special characters (comma, slash).
     */
    public void testOpenPitEncodesSpecialCharactersInIndices() {
        String prefix1 = randomAlphaOfLength(between(1, 5));
        String prefix2 = randomAlphaOfLength(between(1, 5));
        Request request = openPit(new String[] { prefix1 + ",", prefix2 + "/" }, randomPositiveTimeValue(), null);
        assertEquals("POST", request.getMethod());
        assertEquals("/" + prefix1 + "%2C," + prefix2 + "%2F/_pit", request.getEndpoint());
        assertThat(request.getParameters(), hasEntry("allow_partial_search_results", "false"));
    }

    /**
     * Verifies that openPit passes through various TimeValue formats for keep_alive.
     */
    public void testOpenPitKeepAliveParameter() {
        String index = randomAlphaOfLength(between(1, 10));
        long millis = between(1, 100000);
        var params = openPit(new String[] { index }, timeValueMillis(millis), null).getParameters();
        assertThat(params, hasEntry("allow_partial_search_results", "false"));
        assertThat(params, hasEntry("keep_alive", TimeValue.timeValueMillis(millis).getStringRep()));
        int minutes = between(1, 60);
        assertThat(
            openPit(new String[] { index }, TimeValue.timeValueMinutes(minutes), null).getParameters(),
            hasEntry("keep_alive", TimeValue.timeValueMinutes(minutes).getStringRep())
        );
        int hours = between(1, 24);
        assertThat(
            openPit(new String[] { index }, TimeValue.timeValueHours(hours), null).getParameters(),
            hasEntry("keep_alive", TimeValue.timeValueHours(hours).getStringRep())
        );
    }

    /**
     * Verifies that closePit builds a DELETE request to /_pit with a JSON body containing the base64-encoded PIT id.
     */
    public void testClosePitRequestStructure() throws IOException {
        byte[] pitIdBytes = randomByteArrayOfLength(between(1, 64));
        BytesReference pitId = new BytesArray(pitIdBytes);
        Request request = closePit(pitId);
        assertEquals("DELETE", request.getMethod());
        assertEquals("/_pit", request.getEndpoint());
        assertThat(request.getEntity(), not(nullValue()));
        assertEquals(ContentType.APPLICATION_JSON.toString(), request.getEntity().getContentType().getValue());
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        String expectedId = Base64.getUrlEncoder().encodeToString(pitIdBytes);
        assertThat(body, containsString("\"id\":\"" + expectedId + "\""));
    }

    /**
     * Verifies that closePit correctly encodes the PIT id for binary-like content.
     */
    public void testClosePitEncodesBinaryPitId() throws IOException {
        byte[] pitIdBytes = randomByteArrayOfLength(between(1, 32));
        BytesReference pitId = new BytesArray(pitIdBytes);
        Request request = closePit(pitId);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        String expectedId = Base64.getUrlEncoder().encodeToString(pitIdBytes);
        assertThat(body, containsString("\"id\":\"" + expectedId + "\""));
    }

    /**
     * Verifies that closePit produces valid JSON with an id field containing the base64-encoded PIT id.
     */
    public void testClosePitProducesValidJson() throws IOException {
        String pitIdStr = randomAlphaOfLength(between(1, 50));
        BytesReference pitId = new BytesArray(pitIdStr.getBytes(StandardCharsets.UTF_8));
        Request request = closePit(pitId);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        String expectedId = Base64.getUrlEncoder().encodeToString(pitIdStr.getBytes(StandardCharsets.UTF_8));
        assertThat(body, containsString("\"id\""));
        assertThat(body, containsString(expectedId));
        assertThat(body.trim(), startsWith("{"));
        assertThat(body.trim(), endsWith("}"));
    }

    /**
     * Verifies that pitSearch builds a POST request to /_search with correct method and endpoint.
     */
    public void testPitSearchRequestStructure() {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        // BytesReference query = new BytesArray("{}");
        // BytesReference pitId = new BytesArray("pit-id".getBytes(StandardCharsets.UTF_8));
        // TimeValue keepAlive = randomPositiveTimeValue();
        // Version remoteVersion = Version.fromId(between(6030099, Version.CURRENT.id));

        // Request request = pitSearch(searchRequest, query, pitId, keepAlive, null, remoteVersion);
        Request request = pitSearchWithDefaults(searchRequest, null);
        assertEquals("POST", request.getMethod());
        assertEquals("/_search", request.getEndpoint());
    }

    /**
     * Verifies that pitSearch throws when remote version is before 7.10.0.
     */
    public void testPitSearchRejectsVersionBefore710() {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        BytesReference query = new BytesArray("{}");
        BytesReference pitId = new BytesArray("pit".getBytes(StandardCharsets.UTF_8));
        TimeValue keepAlive = timeValueMillis(60000);

        Version versionBefore710 = Version.fromId(between(0, Version.V_7_10_0.id - 1));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> pitSearch(searchRequest, query, pitId, keepAlive, null, versionBefore710)
        );
        assertThat(e.getMessage(), containsString("PIT search requires remote version 7.10.0 or later"));
        assertThat(e.getMessage(), containsString(versionBefore710.toString()));
    }

    /**
     * Verifies that pitSearch adds allow_partial_search_results=false for versions 7.10.0+.
     */
    public void testPitSearchAllowPartialParameter() {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        BytesReference query = new BytesArray("{}");
        BytesReference pitId = new BytesArray("pit".getBytes(StandardCharsets.UTF_8));
        TimeValue keepAlive = timeValueMillis(60000);

        Version version710OrLater = Version.fromId(between(Version.V_7_10_0.id, Version.CURRENT.id));
        assertThat(
            pitSearch(searchRequest, query, pitId, keepAlive, null, version710OrLater).getParameters().get("allow_partial_search_results"),
            equalTo("false")
        );
    }

    /**
     * Verifies that pitSearch body contains pit.id (base64) and pit.keep_alive.
     */
    public void testPitSearchRequestBody() throws IOException {
        byte[] pitIdBytes = randomByteArrayOfLength(between(1, 32));
        BytesReference pitId = new BytesArray(pitIdBytes);
        TimeValue keepAlive = timeValueMillis(between(1000, 60000));
        int size = between(1, 1000);
        boolean version = randomBoolean();
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().size(size).version(version).sort("_shard_doc"));
        BytesReference query = new BytesArray("{}");
        Version remoteVersion = Version.fromId(between(Version.V_7_10_0.id, Version.CURRENT.id));

        Request request = pitSearch(searchRequest, query, pitId, keepAlive, null, remoteVersion);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        String expectedPitId = Base64.getUrlEncoder().encodeToString(pitIdBytes);
        assertThat(body, containsString("\"pit\""));
        assertThat(body, containsString("\"id\":\"" + expectedPitId + "\""));
        assertThat(body, containsString("\"keep_alive\":\"" + keepAlive.getStringRep() + "\""));
        assertThat(body, containsString("\"size\":" + size));
        assertThat(body, containsString("\"version\":" + version));
        assertThat(body, containsString("\"sort\""));
        assertThat(body, containsString("\"query\":"));
    }

    /**
     * Verifies that pitSearch body contains a query when one is set
     */
    public void testPitSearchBodyQuery() throws IOException {
        String queryStr = "{\"match_all\":{}}";
        BytesReference query = new BytesArray(queryStr);
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        BytesReference pitId = new BytesArray("pit".getBytes(StandardCharsets.UTF_8));
        TimeValue keepAlive = timeValueMillis(60000);
        Version remoteVersion = Version.fromId(between(Version.V_7_10_0.id, Version.CURRENT.id));

        Request request = pitSearch(searchRequest, query, pitId, keepAlive, null, remoteVersion);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, containsString("\"query\":{\"match_all\":{}}"));
    }

    /**
     * Verifies that pitSearch body contains _source default when fetchSource is null.
     */
    public void testPitSearchBodySourceDefault() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        Request request = pitSearchWithDefaults(searchRequest, null);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, containsString("\"_source\":true"));
    }

    /**
     * Verifies that pitSearch body contains _source with includes/excludes when fetchSource is set.
     */
    public void testPitSearchBodySourceWithIncludesExcludes() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().fetchSource(new String[] { "in1", "in2" }, new String[] { "out" })
        );
        Request request = pitSearchWithDefaults(searchRequest, null);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, containsString("\"_source\""));
        assertThat(body, containsString("in1"));
        assertThat(body, containsString("in2"));
        assertThat(body, containsString("out"));
    }

    /**
     * Verifies that pitSearch body contains search_after when provided and non-empty.
     */
    public void testPitSearchBodySearchAfter() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());

        Request requestWithoutSearchAfter = pitSearchWithDefaults(searchRequest, null);
        String bodyWithout = Streams.copyToString(
            new InputStreamReader(requestWithoutSearchAfter.getEntity().getContent(), StandardCharsets.UTF_8)
        );
        assertThat(bodyWithout, not(containsString("search_after")));

        Request requestWithEmptySearchAfter = pitSearchWithDefaults(searchRequest, new Object[0]);
        String bodyEmpty = Streams.copyToString(
            new InputStreamReader(requestWithEmptySearchAfter.getEntity().getContent(), StandardCharsets.UTF_8)
        );
        assertThat(bodyEmpty, not(containsString("search_after")));

        Object[] searchAfter = new Object[] { 4294967396L, "sort-key" };
        Request requestWithSearchAfter = pitSearchWithDefaults(searchRequest, searchAfter);
        String bodyWith = Streams.copyToString(
            new InputStreamReader(requestWithSearchAfter.getEntity().getContent(), StandardCharsets.UTF_8)
        );
        assertThat(bodyWith, containsString("\"search_after\""));
        assertThat(bodyWith, containsString("4294967396"));
        assertThat(bodyWith, containsString("sort-key"));
    }

    /**
     * PIT search must not send {@code project_routing}; scope is fixed when the PIT was opened (see {@code openPit}).
     */
    public void testPitSearchBodyOmitsProjectRoutingWhenSetOnSearchRequest() throws IOException {
        String projectRouting = "_alias:linked";
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        searchRequest.setProjectRouting(projectRouting);
        Request request = pitSearchWithDefaults(searchRequest, null);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, not(containsString("project_routing")));
    }

    /**
     * Verifies that openPit puts {@code project_routing} in the request body when provided (cross-project open PIT).
     */
    public void testOpenPitBodyIncludesProjectRoutingWhenProvided() throws IOException {
        String index = randomAlphaOfLength(between(1, 20));
        TimeValue keepAlive = randomPositiveTimeValue();
        String projectRouting = "_alias:linked";
        Request request = openPit(new String[] { index }, keepAlive, projectRouting);
        assertNotNull(request.getEntity());
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, containsString("\"project_routing\":\"" + projectRouting + "\""));
    }

    /**
     * Verifies that pitSearch body omits project_routing when not set.
     */
    public void testPitSearchBodyOmitsProjectRoutingWhenNull() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        Request request = pitSearchWithDefaults(searchRequest, null);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, not(containsString("project_routing")));
    }

    /**
     * Verifies that pitSearch throws when query is malformed or contains multiple objects.
     */
    public void testPitSearchInvalidQuery() {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        BytesReference pitId = new BytesArray("pit".getBytes(StandardCharsets.UTF_8));
        TimeValue keepAlive = timeValueMillis(60000);
        Version remoteVersion = Version.fromId(between(Version.V_7_10_0.id, Version.CURRENT.id));

        RuntimeException e = expectThrows(
            RuntimeException.class,
            () -> pitSearch(searchRequest, new BytesArray("{,"), pitId, keepAlive, null, remoteVersion)
        );
        assertThat(e.getCause().getMessage(), containsString("Unexpected character"));

        e = expectThrows(
            RuntimeException.class,
            () -> pitSearch(searchRequest, new BytesArray("{"), pitId, keepAlive, null, remoteVersion)
        );
        assertThat(e.getCause().getMessage(), containsString("Unexpected end-of-input"));
    }

    /**
     * Verifies that pitSearch throws when query contains more than a single JSON object
     * (e.g. concatenated objects like {"a":1}{"b":2} which Jackson parses without failing).
     */
    public void testPitSearchThrowsWhenQueryHasMultipleObjects() {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        BytesReference pitId = new BytesArray("pit".getBytes(StandardCharsets.UTF_8));
        TimeValue keepAlive = timeValueMillis(60000);
        Version remoteVersion = Version.fromId(between(Version.V_7_10_0.id, Version.CURRENT.id));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> pitSearch(searchRequest, new BytesArray("{\"match_all\":{}}{\"x\":1}"), pitId, keepAlive, null, remoteVersion)
        );
        assertThat(e.getMessage(), containsString("query was more than a single object"));
    }

    /**
     * Verifies that pitSearch uses manual _source serialization for versions before 9.1
     * when fetchSource has excludeVectors set and no includes/excludes.
     */
    public void testPitSearchBodySourceExcludeVectorsBefore91WithNoIncludesExcludes() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().fetchSource(FetchSourceContext.of(true, true, null, null))
        );
        Version remoteVersion = Version.fromId(between(Version.V_7_10_0.id, Version.V_9_1_0.id - 1));

        Request request = pitSearchWithDefaults(searchRequest, null, remoteVersion);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, containsString("\"_source\":true"));
        assertThat(body, not(containsString("exclude_vectors")));
    }

    /**
     * Verifies that pitSearch uses manual _source serialization with includes/excludes
     * for versions before 9.1 when fetchSource has excludeVectors set.
     */
    public void testPitSearchBodySourceExcludeVectorsBefore91WithIncludesExcludes() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().fetchSource(FetchSourceContext.of(true, true, new String[] { "in1", "in2" }, new String[] { "out" }))
        );
        Version remoteVersion = Version.fromId(between(Version.V_7_10_0.id, Version.V_9_1_0.id - 1));

        Request request = pitSearchWithDefaults(searchRequest, null, remoteVersion);
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body, containsString("\"_source\""));
        assertThat(body, containsString("in1"));
        assertThat(body, containsString("in2"));
        assertThat(body, containsString("out"));
        assertThat(body, not(containsString("exclude_vectors")));
    }

    /**
     * Verifies that pitSearch builds valid JSON entity with correct content type.
     */
    public void testPitSearchEntityContentType() throws IOException {
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder());
        Request request = pitSearchWithDefaults(searchRequest, null);
        assertEquals(ContentType.APPLICATION_JSON.toString(), request.getEntity().getContentType().getValue());
        String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
        assertThat(body.trim(), startsWith("{"));
        assertThat(body.trim(), endsWith("}"));
    }

    private Request pitSearchWithDefaults(SearchRequest searchRequest, @Nullable Object[] searchAfter) {
        return pitSearchWithDefaults(searchRequest, searchAfter, Version.fromId(between(Version.V_7_10_0.id, Version.CURRENT.id)));
    }

    private Request pitSearchWithDefaults(SearchRequest searchRequest, @Nullable Object[] searchAfter, Version version) {
        return pitSearch(
            searchRequest,
            new BytesArray("{}"),
            new BytesArray("pit".getBytes(StandardCharsets.UTF_8)),
            timeValueMillis(60000),
            searchAfter,
            version
        );
    }
}
