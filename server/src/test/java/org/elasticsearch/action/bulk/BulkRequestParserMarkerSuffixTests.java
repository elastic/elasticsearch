/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class BulkRequestParserMarkerSuffixTests extends BulkRequestParserTestCase {

    @Override
    protected XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    protected RestBulkAction.BulkFormat bulkFormat() {
        return RestBulkAction.BulkFormat.MARKER_SUFFIX;
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
                contentType(),
                RestBulkAction.BulkFormat.MARKER_SUFFIX,
                (req, type) -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far")
            )
        );
        assertEquals("[1:14] Unexpected end of file", ex.getMessage());
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
                contentType(),
                RestBulkAction.BulkFormat.MARKER_SUFFIX,
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
            contentType(),
            bulkFormat(),
            (req, type) -> {},
            req -> {},
            req -> {}
        );

        // Should not throw because not last
        incrementalParser.parse(request, false);

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> incrementalParser.parse(request, true));
        assertEquals("The bulk request must be terminated by a newline [\\n]", e2.getMessage());
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
                contentType(),
                RestBulkAction.BulkFormat.MARKER_SUFFIX,
                (req, type) -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far"),
                req -> fail("expected failure before we got this far")
            )
        );
        assertEquals("Malformed action/metadata line [1], unexpected data after the closing brace", ex.getMessage());
    }
}
