/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DocumentParserListenerTests extends MapperServiceTestCase {
    private static class MemorizingDocumentParserListener implements DocumentParserListener {
        private final List<Event> events = new ArrayList<>();
        private final List<Token> tokens = new ArrayList<>();

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public void consume(Token token) throws IOException {
            // Tokens contains information tied to current parser state so we need to "materialize" them.
            if (token instanceof Token.StringAsCharArrayValue charArray) {
                var string = String.copyValueOf(charArray.buffer(), charArray.offset(), charArray.length());
                tokens.add((Token.ValueToken<?>) () -> string);
            } else if (token instanceof Token.ValueToken<?> v) {
                var value = v.value();
                tokens.add((Token.ValueToken<?>) () -> value);
            } else {
                tokens.add(token);
            }
        }

        @Override
        public void consume(Event event) throws IOException {
            events.add(event);
        }

        @Override
        public Output finish() {
            return new Output(List.of());
        }

        public List<Token> getTokens() {
            return tokens;
        }

        public List<Event> getEvents() {
            return events;
        }
    }

    public void testEventFlow() throws IOException {
        var mapping = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().startObject("_doc").startObject("properties");
        {
            mapping.startObject("leaf").field("type", "keyword").endObject();
            mapping.startObject("leaf_array").field("type", "keyword").endObject();

            mapping.startObject("object").startObject("properties");
            {
                mapping.startObject("leaf").field("type", "keyword").endObject();
                mapping.startObject("leaf_array").field("type", "keyword").endObject();
            }
            mapping.endObject().endObject();

            mapping.startObject("object_array").startObject("properties");
            {
                mapping.startObject("leaf").field("type", "keyword").endObject();
                mapping.startObject("leaf_array").field("type", "keyword").endObject();
            }
            mapping.endObject().endObject();
        }
        mapping.endObject().endObject().endObject();
        var mappingService = createSytheticSourceMapperService(mapping);

        XContentType xContentType = randomFrom(XContentType.values());

        var listener = new MemorizingDocumentParserListener();
        var documentParser = new DocumentParser(
            XContentParserConfiguration.EMPTY,
            mappingService.parserContext(),
            (ml, xct) -> new DocumentParser.Listeners.Single(listener)
        );

        var source = XContentBuilder.builder(xContentType.xContent());
        source.startObject();
        {
            source.field("leaf", "leaf");
            source.array("leaf_array", "one", "two");
            source.startObject("object");
            {
                source.field("leaf", "leaf");
                source.array("leaf_array", "one", "two");
            }
            source.endObject();
            source.startArray("object_array");
            {
                source.startObject();
                {
                    source.field("leaf", "leaf");
                    source.array("leaf_array", "one", "two");
                }
                source.endObject();
            }
            source.endArray();
        }
        source.endObject();

        documentParser.parseDocument(new SourceToParse("id1", BytesReference.bytes(source), xContentType), mappingService.mappingLookup());
        var events = listener.getEvents();

        assertEquals("_doc", ((DocumentParserListener.Event.DocumentStart) events.get(0)).rootObjectMapper().fullPath());

        assertLeafEvents(events, 1, "", "_doc", false);

        var objectStart = (DocumentParserListener.Event.ObjectStart) events.get(5);
        assertEquals("object", objectStart.objectMapper().fullPath());
        assertEquals("_doc", objectStart.parentMapper().fullPath());
        assertFalse(objectStart.insideObjectArray());
        assertLeafEvents(events, 6, "object.", "object", false);

        var objectArrayStart = (DocumentParserListener.Event.ObjectArrayStart) events.get(10);
        assertEquals("object_array", objectArrayStart.objectMapper().fullPath());
        assertEquals("_doc", objectArrayStart.parentMapper().fullPath());

        var objectInArrayStart = (DocumentParserListener.Event.ObjectStart) events.get(11);
        assertEquals("object_array", objectInArrayStart.objectMapper().fullPath());
        assertEquals("_doc", objectInArrayStart.parentMapper().fullPath());
        assertTrue(objectInArrayStart.insideObjectArray());
        assertLeafEvents(events, 12, "object_array.", "object_array", true);
    }

    public void testTokenFlow() throws IOException {
        var mapping = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("_doc")
            .field("enabled", false)
            .endObject()
            .endObject();
        var mappingService = createSytheticSourceMapperService(mapping);

        XContentType xContentType = randomFrom(XContentType.values());

        var listener = new MemorizingDocumentParserListener();
        var documentParser = new DocumentParser(
            XContentParserConfiguration.EMPTY,
            mappingService.parserContext(),
            (ml, xct) -> new DocumentParser.Listeners.Single(listener)
        );

        var source = XContentBuilder.builder(xContentType.xContent());
        source.startObject();
        {
            source.field("leaf", "leaf");
            source.array("leaf_array", "one", "two");
            source.startObject("object");
            {
                source.field("leaf", "leaf");
                source.array("leaf_array", "one", "two");
            }
            source.endObject();
            source.startArray("object_array");
            {
                source.startObject();
                {
                    source.field("leaf", "leaf");
                    source.array("leaf_array", "one", "two");
                }
                source.endObject();
            }
            source.endArray();
        }
        source.endObject();

        documentParser.parseDocument(new SourceToParse("id1", BytesReference.bytes(source), xContentType), mappingService.mappingLookup());
        var tokens = listener.getTokens();
        assertTrue(tokens.get(0) instanceof DocumentParserListener.Token.StartObject);
        {
            assertLeafTokens(tokens, 1);
            assertEquals("object", ((DocumentParserListener.Token.FieldName) tokens.get(8)).name());
            assertTrue(tokens.get(9) instanceof DocumentParserListener.Token.StartObject);
            {
                assertLeafTokens(tokens, 10);
            }
            assertTrue(tokens.get(17) instanceof DocumentParserListener.Token.EndObject);
            assertEquals("object_array", ((DocumentParserListener.Token.FieldName) tokens.get(18)).name());
            assertTrue(tokens.get(19) instanceof DocumentParserListener.Token.StartArray);
            {
                assertTrue(tokens.get(20) instanceof DocumentParserListener.Token.StartObject);
                {
                    assertLeafTokens(tokens, 21);
                }
                assertTrue(tokens.get(28) instanceof DocumentParserListener.Token.EndObject);
            }
            assertTrue(tokens.get(29) instanceof DocumentParserListener.Token.EndArray);
        }
        assertTrue(tokens.get(30) instanceof DocumentParserListener.Token.EndObject);
    }

    private void assertLeafEvents(
        List<DocumentParserListener.Event> events,
        int start,
        String prefix,
        String parent,
        boolean inObjectArray
    ) {
        var leafValue = (DocumentParserListener.Event.LeafValue) events.get(start);
        assertEquals(prefix + "leaf", leafValue.fieldMapper().fullPath());
        assertEquals(parent, leafValue.parentMapper().fullPath());
        assertFalse(leafValue.isArray());
        assertFalse(leafValue.isContainer());
        assertEquals(inObjectArray, leafValue.insideObjectArray());

        var leafArray = (DocumentParserListener.Event.LeafArrayStart) events.get(start + 1);
        assertEquals(prefix + "leaf_array", leafArray.fieldMapper().fullPath());
        assertEquals(parent, leafArray.parentMapper().fullPath());

        var arrayValue1 = (DocumentParserListener.Event.LeafValue) events.get(start + 2);
        assertEquals(prefix + "leaf_array", arrayValue1.fieldMapper().fullPath());
        assertEquals(parent, arrayValue1.parentMapper().fullPath());
        assertFalse(arrayValue1.isArray());
        assertFalse(arrayValue1.isContainer());
        assertEquals(inObjectArray, leafValue.insideObjectArray());

        var arrayValue2 = (DocumentParserListener.Event.LeafValue) events.get(start + 3);
        assertEquals(prefix + "leaf_array", arrayValue2.fieldMapper().fullPath());
        assertEquals(parent, arrayValue2.parentMapper().fullPath());
        assertFalse(arrayValue2.isArray());
        assertFalse(arrayValue2.isContainer());
        assertEquals(inObjectArray, leafValue.insideObjectArray());
    }

    private void assertLeafTokens(List<DocumentParserListener.Token> tokens, int start) throws IOException {
        assertEquals("leaf", ((DocumentParserListener.Token.FieldName) tokens.get(start)).name());
        assertEquals("leaf", ((DocumentParserListener.Token.ValueToken<?>) tokens.get(start + 1)).value());
        assertEquals("leaf_array", ((DocumentParserListener.Token.FieldName) tokens.get(start + 2)).name());
        assertTrue(tokens.get(start + 3) instanceof DocumentParserListener.Token.StartArray);
        assertEquals("one", ((DocumentParserListener.Token.ValueToken<?>) tokens.get(start + 4)).value());
        assertEquals("two", ((DocumentParserListener.Token.ValueToken<?>) tokens.get(start + 5)).value());
        assertTrue(tokens.get(start + 6) instanceof DocumentParserListener.Token.EndArray);
    }
}
