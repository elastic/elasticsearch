/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class BatchDocumentParserContextTests extends MapperServiceTestCase {

    private BatchDocumentParserContext newContext() throws IOException {
        MapperService ms = createMapperService(mapping(b -> b.startObject("host").field("type", "keyword").endObject()));
        SourceToParse source = new SourceToParse("id1", new BytesArray("{}"), XContentType.JSON);
        return new BatchDocumentParserContext(ms.mappingLookup(), ms.parserContext(), source);
    }

    public void testDocAndRootDocAreSame() throws IOException {
        BatchDocumentParserContext ctx = newContext();
        assertNotNull(ctx.doc());
        assertSame(ctx.doc(), ctx.rootDoc());
    }

    public void testNonRootDocumentsIsEmpty() throws IOException {
        BatchDocumentParserContext ctx = newContext();
        assertFalse(ctx.nonRootDocuments().iterator().hasNext());
    }

    public void testGetTsidDefaultsToNull() throws IOException {
        BatchDocumentParserContext ctx = newContext();
        assertNull(ctx.getTsid());
    }

    public void testAddIgnoredFieldTracked() throws IOException {
        BatchDocumentParserContext ctx = newContext();
        ctx.addIgnoredField("host");
        assertTrue(ctx.getIgnoredFields().contains("host"));
    }

    public void testVersionAndSeqIdInitialState() throws IOException {
        BatchDocumentParserContext ctx = newContext();
        assertNull(ctx.version());
        assertNotNull(ctx.seqID());
    }

    public void testMappingLookupIsInjected() throws IOException {
        MapperService ms = createMapperService(mapping(b -> b.startObject("host").field("type", "keyword").endObject()));
        SourceToParse source = new SourceToParse("id1", new BytesArray("{}"), XContentType.JSON);
        BatchDocumentParserContext ctx = new BatchDocumentParserContext(ms.mappingLookup(), ms.parserContext(), source);
        assertSame(ms.mappingLookup(), ctx.mappingLookup());
    }
}
