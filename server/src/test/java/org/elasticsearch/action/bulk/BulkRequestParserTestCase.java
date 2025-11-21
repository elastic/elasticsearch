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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public abstract class BulkRequestParserTestCase extends ESTestCase {

    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // Replace with just RestApiVersion.values() when V8 no longer exists
    public static final List<RestApiVersion> REST_API_VERSIONS_POST_V8 = Stream.of(RestApiVersion.values())
        .filter(v -> v.matches(RestApiVersion.onOrAfter(RestApiVersion.V_9)))
        .toList();

    protected abstract XContentType contentType();

    protected abstract RestBulkAction.BulkFormat bulkFormat();

    protected BytesArray buildBulk(List<String> docs) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (String doc : docs) {
                BytesArray convertedDoc = convertToFormat(new BytesArray(doc));
                if (bulkFormat() == RestBulkAction.BulkFormat.PREFIX_LENGTH) {
                    out.writeInt(convertedDoc.length());
                }
                out.write(convertedDoc.array(), convertedDoc.arrayOffset(), convertedDoc.length());
                if (bulkFormat() == RestBulkAction.BulkFormat.MARKER_SUFFIX) {
                    out.write(contentType().xContent().bulkSeparator());
                }
            }
            return new BytesArray(out.bytes().toBytesRef());
        }

    }

    protected BytesArray convertToFormat(BytesArray array) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY, array.array(), 0, array.length());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try (XContentBuilder builder = XContentFactory.contentBuilder(contentType(), out)) {
                builder.copyCurrentStructure(parser);
            }
            return new BytesArray(out.bytes().toBytesRef());
        }
    }

    public void testParserCannotBeReusedAfterFailure() throws IOException {
        BytesArray request = buildBulk(List.of("{ \"index\":{ }, \"something\": \"unexpected\" }", "{}"));

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
            contentType(),
            bulkFormat(),
            (req, type) -> fail("expected failure before we got this far"),
            req -> fail("expected failure before we got this far"),
            req -> fail("expected failure before we got this far")
        );

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> incrementalParser.parse(request, false));
        assertEquals("Malformed action/metadata line [1], expected END_OBJECT but found [FIELD_NAME]", ex.getMessage());

        BytesArray valid = buildBulk(List.of("{ \"index\":{ \"_id\": \"bar\" }}", "{}"));
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
            contentType(),
            bulkFormat(),
            (r, t) -> indexRequests.add(r),
            updateRequests::add,
            deleteRequests::add
        );

        BytesArray request = buildBulk(List.of("{ \"index\":{ \"_id\": \"bar\", \"pipeline\": \"foo\" } }", """
            { "field": "value"}
            """, """
            { "index":{ "require_alias": false } }
            """, """
            { "field": "value" }
            """, """
            { "update":{ "_id": "bus", "require_alias": true } }
            """, """
            { "doc": {"field": "value" }}
            """, """
            { "delete":{ "_id": "baz" } }
            """, """
            { "index": { } }
            """, """
            { "field": "value"}
            """, """
            { "delete":{ "_id": "bop" } }
            """));

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
        BytesArray request = buildBulk(List.of("""
            { "index":{ "_id": "bar" } }
            """, "{}"));

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, null, null, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertFalse(indexRequest.isRequireAlias());
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());

        parser.parse(request, "foo", null, null, null, true, null, null, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertTrue(indexRequest.isRequireAlias());
        }, req -> fail(), req -> fail());

        request = buildBulk(List.of("""
            { "index":{ "_id": "bar", "require_alias": true } }
            """, "{}"));

        parser.parse(request, "foo", null, null, null, null, null, null, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertTrue(indexRequest.isRequireAlias());
        }, req -> fail(), req -> fail());

        request = buildBulk(List.of("""
            { "index":{ "_id": "bar", "require_alias": false } }
            """, " {}"));
        parser.parse(request, "foo", null, null, null, true, null, null, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertFalse(indexRequest.isRequireAlias());
        }, req -> fail(), req -> fail());
    }

    public void testDeleteRequest() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "delete":{ "_id": "bar" } }
            """));
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
            contentType(),
            bulkFormat(),
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
        BytesArray request = buildBulk(List.of("""
            { "update":{ "_id": "bar" } }
            """, "{}"));

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
            contentType(),
            bulkFormat(),
            (req, type) -> fail(),
            updateRequest -> {
                assertFalse(parsed.get());
                assertEquals("foo", updateRequest.index());
                assertEquals("bar", updateRequest.id());
                assertFalse(updateRequest.isRequireAlias());
                parsed.set(true);
            },
            req -> fail()
        );
        assertTrue(parsed.get());

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            true,
            null,
            null,
            false,
            contentType(),
            bulkFormat(),
            (req, type) -> fail(),
            updateRequest -> {
                assertTrue(updateRequest.isRequireAlias());
            },
            req -> fail()
        );

        request = buildBulk(List.of("""
            { "update":{ "_id": "bar", "require_alias": true } }
            """, """
            {}
            """));
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
            contentType(),
            bulkFormat(),
            (req, type) -> fail(),
            updateRequest -> {
                assertTrue(updateRequest.isRequireAlias());
            },
            req -> fail()
        );

        request = buildBulk(List.of("""
            { "update":{ "_id": "bar", "require_alias": false } }
            """, """
            {}
            """));
        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            true,
            null,
            null,
            false,
            contentType(),
            bulkFormat(),
            (req, type) -> fail(),
            updateRequest -> {
                assertFalse(updateRequest.isRequireAlias());
            },
            req -> fail()
        );
    }

    public void testFailOnExplicitIndex() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "index":{ "_index": "foo", "_id": "bar" } }
            """, """
            {}
            """));
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
                contentType(),
                bulkFormat(),
                (req, type) -> fail(),
                req -> fail(),
                req -> fail()
            )
        );
        assertEquals("explicit index in bulk is not allowed", ex.getMessage());
    }

    public void testTypesStillParsedForBulkMonitoring() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "index":{ "_type": "quux", "_id": "bar" } }
            """, """
            {}
            """));
        BulkRequestParser parser = new BulkRequestParser(false, true, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, null, null, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertEquals("quux", type);
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());
    }

    public void testParseDeduplicatesParameterStrings() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "index":{ "_index": "bar", "pipeline": "foo", "routing": "blub"} }
            """, "{}", """
            { "index":{ "_index": "bar", "pipeline": "foo", "routing": "blub" } }
            """, "{}"));
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
            contentType(),
            bulkFormat(),
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

    public void testFailOnInvalidAction() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "invalidaction":{ } }
            """, """
            {}
            """));
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
                contentType(),
                bulkFormat(),
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

    public void testFailExtraKeys() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "index":{ }, "something": "unexpected" }
            """, """
            {}
            """));
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
                contentType(),
                bulkFormat(),
                (req, type) -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far")
            )
        );
        assertEquals("Malformed action/metadata line [1], expected END_OBJECT but found [FIELD_NAME]", ex.getMessage());
    }

    public void testListExecutedPipelines() throws IOException {
        BytesArray request = buildBulk(List.of("""
            { "index":{ "_id": "bar" } }
            """, """
            {}
            """));
        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), true, RestApiVersion.current());
        parser.parse(request, "foo", null, null, null, null, null, null, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertFalse(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        parser.parse(request, "foo", null, null, null, null, null, true, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = buildBulk(List.of("""
            { "index":{ "_id": "bar", "op_type": "create" } }
            """, """
            {}
            """));
        parser.parse(request, "foo", null, null, null, null, null, true, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = buildBulk(List.of("""
            { "create":{ "_id": "bar" } }
            """, """
            {}
            """));
        parser.parse(request, "foo", null, null, null, null, null, true, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = buildBulk(List.of("""
            { "index":{ "_id": "bar", "list_executed_pipelines": "true" } }
            """, """
            {}
            """));
        parser.parse(request, "foo", null, null, null, null, null, false, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertTrue(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());

        request = buildBulk(List.of("""
            { "index":{ "_id": "bar", "list_executed_pipelines": "false" } }
            """, """
            {}
            """));
        parser.parse(request, "foo", null, null, null, null, null, true, false, contentType(), bulkFormat(), (indexRequest, type) -> {
            assertFalse(indexRequest.getListExecutedPipelines());
        }, req -> fail(), req -> fail());
    }
}
