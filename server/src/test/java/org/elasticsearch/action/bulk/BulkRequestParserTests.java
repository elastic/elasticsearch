/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class BulkRequestParserTests extends ESTestCase {

    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // Replace with just RestApiVersion.values() when V8 no longer exists
    public static final List<RestApiVersion> REST_API_VERSIONS_POST_V8 = Stream.of(RestApiVersion.values())
        .filter(v -> v.matches(RestApiVersion.onOrAfter(RestApiVersion.V_9)))
        .toList();

    public void testParserCannotBeReusedAfterFailure() {
        BytesArray request = new BytesArray("""
            { "index":{ }, "something": "unexpected" }
            {}
            """);

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        BulkRequestParser.IncrementalParser incrementalParser = parser.incrementalParser(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            XContentType.JSON,
            (req, type) -> fail("expected failure before we got this far"),
            req -> fail("expected failure before we got this far"),
            req -> fail("expected failure before we got this far")
        );

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> incrementalParser.parse(request, false));
        assertEquals("Malformed action/metadata line [1], expected END_OBJECT but found [FIELD_NAME]", ex.getMessage());

        BytesArray valid = new BytesArray("""
            { "index":{ "_id": "bar" } }
            {}
            """);
        expectThrows(AssertionError.class, () -> incrementalParser.parse(valid, false));
    }

    public void testIncrementalParsing() throws IOException {
        ArrayList<DocWriteRequest<?>> indexRequests = new ArrayList<>();
        ArrayList<DocWriteRequest<?>> updateRequests = new ArrayList<>();
        ArrayList<DocWriteRequest<?>> deleteRequests = new ArrayList<>();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        BulkRequestParser.IncrementalParser incrementalParser = parser.incrementalParser(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            XContentType.JSON,
            (r, t) -> indexRequests.add(r),
            updateRequests::add,
            deleteRequests::add
        );

        BytesArray request = new BytesArray("""
            { "index":{ "_id": "bar", "pipeline": "foo" } }
            { "field": "value"}
            { "index":{ "require_alias": false } }
            { "field": "value" }
            { "update":{ "_id": "bus", "require_alias": true } }
            { "doc": {"field": "value" }}
            { "delete":{ "_id": "baz" } }
            { "index": { } }
            { "field": "value"}
            { "delete":{ "_id": "bop" } }
            """);

        int consumed = 0;
        for (int i = 0; i < request.length() - 1; ++i) {
            consumed += incrementalParser.parse(request.slice(consumed, i - consumed + 1), false);
        }
        consumed += incrementalParser.parse(request.slice(consumed, request.length() - consumed), true);
        assertThat(consumed, equalTo(request.length()));

        assertThat(indexRequests.size(), equalTo(3));
        assertThat(updateRequests.size(), equalTo(1));
        assertThat(deleteRequests.size(), equalTo(2));
    }

    public void testIndexRequest() throws IOException {
        BytesArray request = new BytesArray("""
            { "index":{ "_id": "bar" } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, null, null, false, XContentType.JSON, (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertFalse(indexRequest.isRequireAlias());
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());

        parser.parse(request, "foo", null, null, null, true, null, null, false, XContentType.JSON, (indexRequest, type) -> {
            assertTrue(indexRequest.isRequireAlias());
        }, req -> fail(), req -> fail());

        request = new BytesArray("""
            { "index":{ "_id": "bar", "require_alias": true } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, null, null, null, false, XContentType.JSON, (indexRequest, type) -> {
            assertTrue(indexRequest.isRequireAlias());
        }, req -> fail(), req -> fail());

        request = new BytesArray("""
            { "index":{ "_id": "bar", "require_alias": false } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, true, null, null, false, XContentType.JSON, (indexRequest, type) -> {
            assertFalse(indexRequest.isRequireAlias());
        }, req -> fail(), req -> fail());
    }

    public void testDeleteRequest() throws IOException {
        BytesArray request = new BytesArray("""
            { "delete":{ "_id": "bar" } }
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            XContentType.JSON,
            (req, type) -> fail(),
            req -> fail(),
            deleteRequest -> {
                assertFalse(parsed.get());
                assertEquals("foo", deleteRequest.index());
                assertEquals("bar", deleteRequest.id());
                parsed.set(true);
            }
        );
        assertTrue(parsed.get());
    }

    public void testUpdateRequest() throws IOException {
        BytesArray request = new BytesArray("""
            { "update":{ "_id": "bar" } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, null, null, false, XContentType.JSON, (req, type) -> fail(), updateRequest -> {
            assertFalse(parsed.get());
            assertEquals("foo", updateRequest.index());
            assertEquals("bar", updateRequest.id());
            assertFalse(updateRequest.isRequireAlias());
            parsed.set(true);
        }, req -> fail());
        assertTrue(parsed.get());

        parser.parse(request, "foo", null, null, null, true, null, null, false, XContentType.JSON, (req, type) -> fail(), updateRequest -> {
            assertTrue(updateRequest.isRequireAlias());
        }, req -> fail());

        request = new BytesArray("""
            { "update":{ "_id": "bar", "require_alias": true } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, null, null, null, false, XContentType.JSON, (req, type) -> fail(), updateRequest -> {
            assertTrue(updateRequest.isRequireAlias());
        }, req -> fail());

        request = new BytesArray("""
            { "update":{ "_id": "bar", "require_alias": false } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, true, null, null, false, XContentType.JSON, (req, type) -> fail(), updateRequest -> {
            assertFalse(updateRequest.isRequireAlias());
        }, req -> fail());
    }

    public void testBarfOnLackOfTrailingNewline() throws IOException {
        BytesArray request = new BytesArray("""
            { "index":{ "_id": "bar" } }
            {}""");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                "foo",
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                XContentType.JSON,
                (req, type) -> fail(),
                req -> fail(),
                req -> fail()
            )
        );
        assertEquals("The bulk request must be terminated by a newline [\\n]", e.getMessage());

        BulkRequestParser.IncrementalParser incrementalParser = parser.incrementalParser(
            "foo",
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            XContentType.JSON,
            (req, type) -> {},
            req -> {},
            req -> {}
        );

        // Should not throw because not last
        incrementalParser.parse(request, false);

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> incrementalParser.parse(request, true));
        assertEquals("The bulk request must be terminated by a newline [\\n]", e2.getMessage());
    }

    public void testFailOnExplicitIndex() {
        BytesArray request = new BytesArray("""
            { "index":{ "_index": "foo", "_id": "bar" } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                XContentType.JSON,
                (req, type) -> fail(),
                req -> fail(),
                req -> fail()
            )
        );
        assertEquals("explicit index in bulk is not allowed", ex.getMessage());
    }

    public void testTypesStillParsedForBulkMonitoring() throws IOException {
        BytesArray request = new BytesArray("""
            { "index":{ "_type": "quux", "_id": "bar" } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(false, true, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, null, null, false, XContentType.JSON, (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertEquals("quux", type);
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());
    }

    public void testParseDeduplicatesParameterStrings() throws IOException {
        BytesArray request = new BytesArray("""
            { "index":{ "_index": "bar", "pipeline": "foo", "routing": "blub"} }
            {}
            { "index":{ "_index": "bar", "pipeline": "foo", "routing": "blub" } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        final List<IndexRequest> indexRequests = new ArrayList<>();
        parser.parse(
            request,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            XContentType.JSON,
            (indexRequest, type) -> indexRequests.add(indexRequest),
            req -> fail(),
            req -> fail()
        );
        assertThat(indexRequests, Matchers.hasSize(2));
        final IndexRequest first = indexRequests.get(0);
        final IndexRequest second = indexRequests.get(1);
        assertSame(first.index(), second.index());
        assertSame(first.getPipeline(), second.getPipeline());
        assertSame(first.routing(), second.routing());
    }

    public void testFailOnInvalidAction() {
        BytesArray request = new BytesArray("""
            { "invalidaction":{ } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, randomFrom(RestApiVersion.values()));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                XContentType.JSON,
                (req, type) -> fail(),
                req -> fail(),
                req -> fail()
            )
        );
        assertEquals(
            "Malformed action/metadata line [1], expected field [create], [delete], [index] or [update] but found [invalidaction]",
            ex.getMessage()
        );
    }

    public void testFailMissingCloseBrace() {
        BytesArray request = new BytesArray("""
            { "index":{ }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, randomFrom(REST_API_VERSIONS_POST_V8));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                XContentType.JSON,
                (req, type) -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far")
            )
        );
        assertEquals("[1:14] Unexpected end of file", ex.getMessage());
    }

    public void testFailExtraKeys() {
        BytesArray request = new BytesArray("""
            { "index":{ }, "something": "unexpected" }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, randomFrom(REST_API_VERSIONS_POST_V8));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                XContentType.JSON,
                (req, type) -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far")
            )
        );
        assertEquals("Malformed action/metadata line [1], expected END_OBJECT but found [FIELD_NAME]", ex.getMessage());
    }

    public void testFailContentAfterClosingBrace() {
        BytesArray request = new BytesArray("""
            { "index":{ } } { "something": "unexpected" }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, randomFrom(REST_API_VERSIONS_POST_V8));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                XContentType.JSON,
                (req, type) -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far")
            )
        );
        assertEquals("Malformed action/metadata line [1], unexpected data after the closing brace", ex.getMessage());
    }

    public void testListExecutedPipelines() throws IOException {
        BytesArray request = new BytesArray("""
            { "index":{ "_id": "bar" } }
            {}
            """);
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        parser.parse(request, "foo", null, null, null, null, null, null, false, XContentType.JSON, (indexRequest, type) -> {
            assertFalse(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        parser.parse(request, "foo", null, null, null, null, null, true, false, XContentType.JSON, (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = new BytesArray("""
            { "index":{ "_id": "bar", "op_type": "create" } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, null, null, true, false, XContentType.JSON, (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = new BytesArray("""
            { "create":{ "_id": "bar" } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, null, null, true, false, XContentType.JSON, (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = new BytesArray("""
            { "index":{ "_id": "bar", "list_executed_pipelines": "true" } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, null, null, false, false, XContentType.JSON, (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = new BytesArray("""
            { "index":{ "_id": "bar", "list_executed_pipelines": "false" } }
            {}
            """);
        parser.parse(request, "foo", null, null, null, null, null, true, false, XContentType.JSON, (indexRequest, type) -> {
            assertFalse(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());
    }

}
