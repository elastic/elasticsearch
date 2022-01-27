/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBulkRequestParserTests extends ESTestCase {

    protected abstract XContentBuilder getBuilder() throws IOException;

    protected abstract XContentType getXContentType();

    public void testIndexRequest() throws IOException {
        XContentBuilder builderIndex = getBuilder().startObject().startObject("index").field("_id", "bar").endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();

        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());

        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, getXContentType(), (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertFalse(indexRequest.isRequireAlias());
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            true,
            false,
            getXContentType(),
            (indexRequest, type) -> { assertTrue(indexRequest.isRequireAlias()); },
            req -> fail(),
            req -> fail()
        );

        builderIndex = getBuilder();
        builderIndex.startObject().startObject("index").field("_id", "bar").field("require_alias", true).endObject().endObject();

        out.reset();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        request = out.bytes();

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            null,
            false,
            getXContentType(),
            (indexRequest, type) -> { assertTrue(indexRequest.isRequireAlias()); },
            req -> fail(),
            req -> fail()
        );

        builderIndex = getBuilder();
        builderIndex.startObject().startObject("index").field("_id", "bar").field("require_alias", false).endObject().endObject();

        out.reset();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        request = out.bytes();

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            true,
            false,
            getXContentType(),
            (indexRequest, type) -> { assertFalse(indexRequest.isRequireAlias()); },
            req -> fail(),
            req -> fail()
        );
    }

    public void testDeleteRequest() throws IOException {
        XContentBuilder builderIndex = getBuilder().startObject().startObject("delete").field("_id", "bar").endObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();
        writeXContent(out, builderIndex);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            null,
            false,
            getXContentType(),
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
        XContentBuilder builderIndex = getBuilder();
        builderIndex.startObject().startObject("update").field("_id", "bar").endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, getXContentType(), (req, type) -> fail(), updateRequest -> {
            assertFalse(parsed.get());
            assertEquals("foo", updateRequest.index());
            assertEquals("bar", updateRequest.id());
            assertFalse(updateRequest.isRequireAlias());
            parsed.set(true);
        }, req -> fail());
        assertTrue(parsed.get());

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            true,
            false,
            getXContentType(),
            (req, type) -> fail(),
            updateRequest -> { assertTrue(updateRequest.isRequireAlias()); },
            req -> fail()
        );

        builderIndex = getBuilder();
        builderIndex.startObject().startObject("update").field("_id", "bar").field("require_alias", true).endObject().endObject();

        out.reset();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        request = out.bytes();

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            null,
            false,
            getXContentType(),
            (req, type) -> fail(),
            updateRequest -> { assertTrue(updateRequest.isRequireAlias()); },
            req -> fail()
        );

        builderIndex = getBuilder();
        builderIndex.startObject().startObject("update").field("_id", "bar").field("require_alias", false).endObject().endObject();

        out.reset();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        request = out.bytes();

        parser.parse(
            request,
            "foo",
            null,
            null,
            null,
            true,
            false,
            getXContentType(),
            (req, type) -> fail(),
            updateRequest -> { assertFalse(updateRequest.isRequireAlias()); },
            req -> fail()
        );
    }

    public void testBarfOnLackOfTrailingNewline() throws IOException {
        XContentBuilder builderIndex = getBuilder();
        builderIndex.startObject().startObject("update").field("_id", "bar").endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();
        writeXContent(out, builderIndex);
        out.write(BytesReference.bytes(builderDocument).array());

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                "foo",
                null,
                null,
                null,
                null,
                false,
                getXContentType(),
                (req, type) -> fail(),
                req -> fail(),
                req -> fail()
            )
        );
        assertEquals("The bulk request must be terminated by a newline [\\n]", e.getMessage());
    }

    public void testFailOnExplicitIndex() throws IOException {
        XContentBuilder builderIndex = getBuilder();
        builderIndex.startObject().startObject("index").field("_index", "foo").field("_id", "bar").endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parse(
                request,
                null,
                null,
                null,
                null,
                null,
                false,
                getXContentType(),
                (req, type) -> fail(),
                req -> fail(),
                req -> fail()
            )
        );
        assertEquals("explicit index in bulk is not allowed", ex.getMessage());
    }

    public void testTypesStillParsedForBulkMonitoring() throws IOException {
        XContentBuilder builderIndex = getBuilder();
        builderIndex.startObject().startObject("index").field("_type", "quux").field("_id", "bar").endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(false, RestApiVersion.current());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, getXContentType(), (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertEquals("quux", type);
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());
    }

    public void testParseDeduplicatesParameterStrings() throws IOException {
        XContentBuilder builderIndex = getBuilder();
        builderIndex.startObject().startObject("index")
            .field("_id", "bar").field("pipeline", "foo").field("routing", "blub")
            .endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().endObject();

        BytesStreamOutput out = new BytesStreamOutput();
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);
        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());
        final List<IndexRequest> indexRequests = new ArrayList<>();
        parser.parse(
            request,
            null,
            null,
            null,
            null,
            null,
            true,
            getXContentType(),
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

    public void testIndexRandomBytes() throws IOException {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 1024));
        XContentBuilder builderIndex = getBuilder().startObject().startObject("index").field("_id", "bar").endObject().endObject();
        XContentBuilder builderDocument = getBuilder().startObject().field("bytes", bytes).endObject();

        BytesStreamOutput out = new BytesStreamOutput();

        writeXContent(out, builderIndex);
        writeXContent(out, builderDocument);

        BytesReference request = out.bytes();

        BulkRequestParser parser = new BulkRequestParser(randomBoolean(), RestApiVersion.current());

        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, getXContentType(), (indexRequest, type) -> {
            assertFalse(parsed.get());
            assertEquals("foo", indexRequest.index());
            assertEquals("bar", indexRequest.id());
            assertFalse(indexRequest.isRequireAlias());
            parsed.set(true);
        }, req -> fail(), req -> fail());
        assertTrue(parsed.get());
    }

    private void writeXContent(StreamOutput out, XContentBuilder builder) throws IOException {
        out.write(BytesReference.bytes(builder).array());
        out.write(getXContentType().xContent().streamSeparator());
    }
}
