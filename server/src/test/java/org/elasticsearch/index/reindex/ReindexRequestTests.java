/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Tests some of the validation of {@linkplain ReindexRequest}. See reindex's rest tests for much more.
 */
public class ReindexRequestTests extends AbstractBulkByScrollRequestTestCase<ReindexRequest> {

    private final BytesReference matchAll = new BytesArray("{ \"foo\" : \"bar\" }");

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected boolean enableWarningsCheck() {
        // There sometimes will be a warning about specifying types in reindex requests being deprecated.
        return false;
    }

    @Override
    protected ReindexRequest createTestInstance() {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("source");
        reindexRequest.setDestIndex("destination");

        if (randomBoolean()) {
            try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint()) {
                BytesReference query = BytesReference.bytes(matchAllQuery().toXContent(builder, ToXContent.EMPTY_PARAMS));
                reindexRequest.setRemoteInfo(
                    new RemoteInfo(
                        randomAlphaOfLength(5),
                        randomAlphaOfLength(5),
                        between(1, Integer.MAX_VALUE),
                        null,
                        query,
                        "user",
                        new SecureString("pass".toCharArray()),
                        emptyMap(),
                        RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                        RemoteInfo.DEFAULT_CONNECT_TIMEOUT
                    )
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        if (randomBoolean()) {
            reindexRequest.setSourceBatchSize(randomInt(100));
        }
        if (randomBoolean()) {
            reindexRequest.setDestOpType("create");
        }
        if (randomBoolean()) {
            reindexRequest.setDestPipeline("my_pipeline");
        }
        if (randomBoolean()) {
            reindexRequest.setDestRouting("=cat");
        }
        if (randomBoolean()) {
            reindexRequest.setMaxDocs(randomIntBetween(100, 1000));
        }
        if (randomBoolean()) {
            reindexRequest.setAbortOnVersionConflict(false);
        }

        if (reindexRequest.getRemoteInfo() == null && randomBoolean()) {
            reindexRequest.setSourceQuery(new TermQueryBuilder("foo", "fooval"));
        }

        return reindexRequest;
    }

    @Override
    protected ReindexRequest doParseInstance(XContentParser parser) throws IOException {
        return ReindexRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(ReindexRequest expectedInstance, ReindexRequest newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertArrayEquals(expectedInstance.getSearchRequest().indices(), newInstance.getSearchRequest().indices());
        assertEquals(expectedInstance.getSearchRequest(), newInstance.getSearchRequest());
        assertEquals(expectedInstance.getMaxDocs(), newInstance.getMaxDocs());
        assertEquals(expectedInstance.getSlices(), newInstance.getSlices());
        assertEquals(expectedInstance.isAbortOnVersionConflict(), newInstance.isAbortOnVersionConflict());
        assertEquals(expectedInstance.getRemoteInfo(), newInstance.getRemoteInfo());
        assertEquals(expectedInstance.getDestination().getPipeline(), newInstance.getDestination().getPipeline());
        assertEquals(expectedInstance.getDestination().routing(), newInstance.getDestination().routing());
        assertEquals(expectedInstance.getDestination().opType(), newInstance.getDestination().opType());
    }

    public void testReindexFromRemoteDoesNotSupportSearchQuery() {
        ReindexRequest reindex = newRequest();
        reindex.setRemoteInfo(
            new RemoteInfo(
                randomAlphaOfLength(5),
                randomAlphaOfLength(5),
                between(1, Integer.MAX_VALUE),
                null,
                matchAll,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            )
        );
        reindex.getSearchRequest().source().query(matchAllQuery()); // Unsupported place to put query
        ActionRequestValidationException e = reindex.validate();
        assertEquals(
            "Validation Failed: 1: reindex from remote sources should use RemoteInfo's query instead of source's query;",
            e.getMessage()
        );
    }

    public void testReindexFromRemoteDoesNotSupportSlices() {
        ReindexRequest reindex = newRequest();
        reindex.setRemoteInfo(
            new RemoteInfo(
                randomAlphaOfLength(5),
                randomAlphaOfLength(5),
                between(1, Integer.MAX_VALUE),
                null,
                matchAll,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            )
        );
        reindex.setSlices(between(2, Integer.MAX_VALUE));
        ActionRequestValidationException e = reindex.validate();
        assertEquals(
            "Validation Failed: 1: reindex from remote sources doesn't support slices > 1 but was [" + reindex.getSlices() + "];",
            e.getMessage()
        );
    }

    public void testNoSliceBuilderSetWithSlicedRequest() {
        ReindexRequest reindex = newRequest();
        reindex.getSearchRequest().source().slice(new SliceBuilder(0, 4));
        reindex.setSlices(between(2, Integer.MAX_VALUE));
        ActionRequestValidationException e = reindex.validate();
        assertEquals("Validation Failed: 1: can't specify both manual and automatic slicing at the same time;", e.getMessage());
    }

    @Override
    protected void extraRandomizationForSlice(ReindexRequest original) {
        if (randomBoolean()) {
            original.setScript(mockScript(randomAlphaOfLength(5)));
        }
        if (randomBoolean()) {
            original.setRemoteInfo(
                new RemoteInfo(
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    between(1, 10000),
                    null,
                    matchAll,
                    null,
                    null,
                    emptyMap(),
                    parseTimeValue(randomPositiveTimeValue(), "socket_timeout"),
                    parseTimeValue(randomPositiveTimeValue(), "connect_timeout")
                )
            );
        }
    }

    @Override
    protected void extraForSliceAssertions(ReindexRequest original, ReindexRequest forSliced) {
        assertEquals(original.getScript(), forSliced.getScript());
        assertEquals(original.getDestination(), forSliced.getDestination());
        assertEquals(original.getRemoteInfo(), forSliced.getRemoteInfo());
    }

    @Override
    protected ReindexRequest newRequest() {
        ReindexRequest reindex = new ReindexRequest();
        reindex.setSourceIndices("source");
        reindex.setDestIndex("dest");
        return reindex;
    }

    public void testBuildRemoteInfoNoRemote() throws IOException {
        assertNull(ReindexRequest.buildRemoteInfo(new HashMap<>()));
    }

    public void testBuildRemoteInfoFullyLoaded() throws IOException {
        Map<String, String> headers = new HashMap<>();
        headers.put("first", "a");
        headers.put("second", "b");
        headers.put("third", "");

        Map<String, Object> remote = new HashMap<>();
        remote.put("host", "https://example.com:9200");
        remote.put("username", "testuser");
        remote.put("password", "testpass");
        remote.put("headers", headers);
        remote.put("socket_timeout", "90s");
        remote.put("connect_timeout", "10s");

        Map<String, Object> query = new HashMap<>();
        query.put("a", "b");

        Map<String, Object> source = new HashMap<>();
        source.put("remote", remote);
        source.put("query", query);

        RemoteInfo remoteInfo = ReindexRequest.buildRemoteInfo(source);
        assertEquals("https", remoteInfo.getScheme());
        assertEquals("example.com", remoteInfo.getHost());
        assertEquals(9200, remoteInfo.getPort());
        assertEquals("""
            {
              "a" : "b"
            }""", remoteInfo.getQuery().utf8ToString());
        assertEquals("testuser", remoteInfo.getUsername());
        assertEquals("testpass", remoteInfo.getPassword().toString());
        assertEquals(headers, remoteInfo.getHeaders());
        assertEquals(timeValueSeconds(90), remoteInfo.getSocketTimeout());
        assertEquals(timeValueSeconds(10), remoteInfo.getConnectTimeout());
    }

    public void testBuildRemoteInfoWithoutAllParts() throws IOException {
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("example.com"));
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase(":9200"));
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("http://:9200"));
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("example.com:9200"));
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("http://example.com"));
    }

    public void testBuildRemoteInfoWithAllHostParts() throws IOException {
        RemoteInfo info = buildRemoteInfoHostTestCase("http://example.com:9200");
        assertEquals("http", info.getScheme());
        assertEquals("example.com", info.getHost());
        assertEquals(9200, info.getPort());
        assertNull(info.getPathPrefix());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout()); // Didn't set the timeout so we should get the default
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout()); // Didn't set the timeout so we should get the default

        info = buildRemoteInfoHostTestCase("https://other.example.com:9201");
        assertEquals("https", info.getScheme());
        assertEquals("other.example.com", info.getHost());
        assertEquals(9201, info.getPort());
        assertNull(info.getPathPrefix());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout());
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout());

        info = buildRemoteInfoHostTestCase("https://[::1]:9201");
        assertEquals("https", info.getScheme());
        assertEquals("[::1]", info.getHost());
        assertEquals(9201, info.getPort());
        assertNull(info.getPathPrefix());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout());
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout());

        info = buildRemoteInfoHostTestCase("https://other.example.com:9201/");
        assertEquals("https", info.getScheme());
        assertEquals("other.example.com", info.getHost());
        assertEquals(9201, info.getPort());
        assertEquals("/", info.getPathPrefix());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout());
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout());

        info = buildRemoteInfoHostTestCase("https://other.example.com:9201/proxy-path/");
        assertEquals("https", info.getScheme());
        assertEquals("other.example.com", info.getHost());
        assertEquals(9201, info.getPort());
        assertEquals("/proxy-path/", info.getPathPrefix());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout());
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout());

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("https"));
        assertEquals("[host] must be of the form [scheme]://[host]:[port](/[pathPrefix])? but was [https]", exception.getMessage());
    }

    public void testReindexFromRemoteRequestParsing() throws IOException {
        BytesReference request;
        try (XContentBuilder b = JsonXContent.contentBuilder()) {
            b.startObject();
            {
                b.startObject("source");
                {
                    b.startObject("remote");
                    {
                        b.field("host", "http://localhost:9200");
                    }
                    b.endObject();
                    b.field("index", "source");
                }
                b.endObject();
                b.startObject("dest");
                {
                    b.field("index", "dest");
                }
                b.endObject();
            }
            b.endObject();
            request = BytesReference.bytes(b);
        }
        try (XContentParser p = createParser(JsonXContent.jsonXContent, request)) {
            ReindexRequest r = ReindexRequest.fromXContent(p);
            assertEquals("localhost", r.getRemoteInfo().getHost());
            assertArrayEquals(new String[] { "source" }, r.getSearchRequest().indices());
        }
    }

    private RemoteInfo buildRemoteInfoHostTestCase(String hostInRest) throws IOException {
        Map<String, Object> remote = new HashMap<>();
        remote.put("host", hostInRest);

        Map<String, Object> source = new HashMap<>();
        source.put("remote", remote);

        return ReindexRequest.buildRemoteInfo(source);
    }

    public void testCommaSeparatedSourceIndices() throws IOException {
        ReindexRequest r = parseRequestWithSourceIndices("a,b");
        assertArrayEquals(new String[] { "a", "b" }, r.getSearchRequest().indices());
    }

    public void testArraySourceIndices() throws IOException {
        ReindexRequest r = parseRequestWithSourceIndices(new String[] { "a", "b" });
        assertArrayEquals(new String[] { "a", "b" }, r.getSearchRequest().indices());
    }

    public void testEmptyStringSourceIndices() throws IOException {
        ReindexRequest r = parseRequestWithSourceIndices("");
        assertArrayEquals(new String[0], r.getSearchRequest().indices());
        ActionRequestValidationException validationException = r.validate();
        assertNotNull(validationException);
        assertEquals(List.of("use _all if you really want to copy from all existing indexes"), validationException.validationErrors());
    }

    private ReindexRequest parseRequestWithSourceIndices(Object sourceIndices) throws IOException {
        BytesReference request;
        try (XContentBuilder b = JsonXContent.contentBuilder()) {
            b.startObject();
            {
                b.startObject("source");
                {
                    b.field("index", sourceIndices);
                }
                b.endObject();
                b.startObject("dest");
                {
                    b.field("index", "dest");
                }
                b.endObject();
            }
            b.endObject();
            request = BytesReference.bytes(b);
        }
        try (XContentParser p = createParser(JsonXContent.jsonXContent, request)) {
            return ReindexRequest.fromXContent(p);
        }
    }
}
