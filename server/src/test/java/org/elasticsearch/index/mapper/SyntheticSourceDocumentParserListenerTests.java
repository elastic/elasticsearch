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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigInteger;

public class SyntheticSourceDocumentParserListenerTests extends MapperServiceTestCase {
    public void testStoreLeafValue() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "long").field("synthetic_source_keep", "all"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var doc = new LuceneDocument();

        var value = XContentBuilder.builder(xContentType.xContent()).value(1234L);
        var parser = createParser(value);
        parser.nextToken();

        sut.consume(
            new DocumentParserListener.Event.LeafValue(
                (FieldMapper) mappingLookup.getMapper("field"),
                false,
                mappingLookup.getMapping().getRoot(),
                doc,
                parser
            )
        );

        var output = sut.finish();

        assertEquals(1, output.ignoredSourceValues().size());
        var valueToStore = output.ignoredSourceValues().get(0);
        assertEquals("field", valueToStore.name());
        var decoded = XContentBuilder.builder(xContentType.xContent());
        XContentDataHelper.decodeAndWrite(decoded, valueToStore.value());
        assertEquals(BytesReference.bytes(value), BytesReference.bytes(decoded));
    }

    public void testStoreLeafArray() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "long").field("synthetic_source_keep", "all"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var values = randomList(0, 10, ESTestCase::randomLong);

        var doc = new LuceneDocument();

        sut.consume(
            new DocumentParserListener.Event.LeafArrayStart(
                (FieldMapper) mappingLookup.getMapper("field"),
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        for (long l : values) {
            sut.consume((DocumentParserListener.Token.ValueToken<Object>) () -> l);
        }
        sut.consume(DocumentParserListener.Token.END_ARRAY);

        var output = sut.finish();

        assertEquals(1, output.ignoredSourceValues().size());
        var valueToStore = output.ignoredSourceValues().get(0);
        assertEquals("field", valueToStore.name());

        var decoded = XContentBuilder.builder(xContentType.xContent());
        XContentDataHelper.decodeAndWrite(decoded, valueToStore.value());

        var parser = createParser(decoded);
        assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
        for (long l : values) {
            parser.nextToken();
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.currentToken());
            assertEquals(l, parser.longValue());
        }
        assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());
    }

    public void testStoreObject() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "object").field("synthetic_source_keep", "all"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var names = randomList(0, 10, () -> randomAlphaOfLength(10));
        var values = randomList(names.size(), names.size(), () -> randomAlphaOfLength(10));

        var doc = new LuceneDocument();

        sut.consume(
            new DocumentParserListener.Event.ObjectStart(
                mappingLookup.objectMappers().get("field"),
                false,
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        for (int i = 0; i < names.size(); i++) {
            sut.consume(new DocumentParserListener.Token.FieldName(names.get(i)));
            var value = values.get(i);
            sut.consume((DocumentParserListener.Token.ValueToken<Object>) () -> value);
        }
        sut.consume(DocumentParserListener.Token.END_OBJECT);

        var output = sut.finish();

        assertEquals(1, output.ignoredSourceValues().size());
        var valueToStore = output.ignoredSourceValues().get(0);
        assertEquals("field", valueToStore.name());

        var decoded = XContentBuilder.builder(xContentType.xContent());
        XContentDataHelper.decodeAndWrite(decoded, valueToStore.value());

        var parser = createParser(decoded);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        for (int i = 0; i < names.size(); i++) {
            parser.nextToken();
            assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken());
            assertEquals(names.get(i), parser.currentName());

            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            assertEquals(values.get(i), parser.text());
        }
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
    }

    public void testStoreObjectArray() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "object").field("synthetic_source_keep", "all"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var names = randomList(0, 10, () -> randomAlphaOfLength(10));
        var values = randomList(names.size(), names.size(), () -> randomAlphaOfLength(10));

        var doc = new LuceneDocument();

        sut.consume(
            new DocumentParserListener.Event.ObjectArrayStart(
                mappingLookup.objectMappers().get("field"),
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        for (int i = 0; i < names.size(); i++) {
            sut.consume(DocumentParserListener.Token.START_OBJECT);

            sut.consume(new DocumentParserListener.Token.FieldName(names.get(i)));
            var value = values.get(i);
            sut.consume((DocumentParserListener.Token.ValueToken<Object>) () -> value);

            sut.consume(DocumentParserListener.Token.END_OBJECT);
        }
        sut.consume(DocumentParserListener.Token.END_ARRAY);

        var output = sut.finish();

        assertEquals(1, output.ignoredSourceValues().size());
        var valueToStore = output.ignoredSourceValues().get(0);
        assertEquals("field", valueToStore.name());

        var decoded = XContentBuilder.builder(xContentType.xContent());
        XContentDataHelper.decodeAndWrite(decoded, valueToStore.value());

        var parser = createParser(decoded);
        assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
        for (int i = 0; i < names.size(); i++) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(names.get(i), parser.currentName());

            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            assertEquals(values.get(i), parser.text());

            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        }
        assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());
    }

    public void testStashedLeafValue() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "boolean").field("synthetic_source_keep", "arrays"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var doc = new LuceneDocument();

        var value = XContentBuilder.builder(xContentType.xContent()).value(false);
        var parser = createParser(value);
        parser.nextToken();

        sut.consume(
            new DocumentParserListener.Event.LeafValue(
                (FieldMapper) mappingLookup.getMapper("field"),
                true,
                mappingLookup.getMapping().getRoot(),
                doc,
                parser
            )
        );

        sut.consume(
            new DocumentParserListener.Event.LeafValue(
                (FieldMapper) mappingLookup.getMapper("field"),
                true,
                mappingLookup.getMapping().getRoot(),
                doc,
                parser
            )
        );

        var output = sut.finish();

        // Single values are optimized away because there are no arrays mixed in and regular synthetic source logic is sufficient
        assertEquals(0, output.ignoredSourceValues().size());
    }

    public void testStashedMixedValues() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "boolean").field("synthetic_source_keep", "arrays"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var doc = new LuceneDocument();

        var value = XContentBuilder.builder(xContentType.xContent()).value(false);
        var parser = createParser(value);
        parser.nextToken();

        sut.consume(
            new DocumentParserListener.Event.LeafValue(
                (FieldMapper) mappingLookup.getMapper("field"),
                true,
                mappingLookup.getMapping().getRoot(),
                doc,
                parser
            )
        );

        sut.consume(
            new DocumentParserListener.Event.LeafValue(
                (FieldMapper) mappingLookup.getMapper("field"),
                true,
                mappingLookup.getMapping().getRoot(),
                doc,
                parser
            )
        );

        sut.consume(
            new DocumentParserListener.Event.LeafArrayStart(
                (FieldMapper) mappingLookup.getMapper("field"),
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        sut.consume((DocumentParserListener.Token.ValueToken<Boolean>) () -> true);
        sut.consume((DocumentParserListener.Token.ValueToken<Boolean>) () -> true);
        sut.consume(DocumentParserListener.Token.END_ARRAY);

        var output = sut.finish();

        // Both arrays and individual values are stored.
        assertEquals(3, output.ignoredSourceValues().size());
    }

    public void testStashedObjectValue() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "object").field("synthetic_source_keep", "arrays"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var doc = new LuceneDocument();

        var value = XContentBuilder.builder(xContentType.xContent()).value(1234L);
        var parser = createParser(value);
        parser.nextToken();

        sut.consume(
            new DocumentParserListener.Event.ObjectStart(
                mappingLookup.objectMappers().get("field"),
                true,
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        sut.consume(new DocumentParserListener.Token.FieldName("hello"));
        sut.consume((DocumentParserListener.Token.ValueToken<BigInteger>) () -> BigInteger.valueOf(13));
        sut.consume(DocumentParserListener.Token.END_OBJECT);

        var output = sut.finish();

        // Single value optimization does not work for objects because it is possible that one of the fields
        // of this object needs to be stored in ignored source.
        // Because we stored the entire object we didn't store individual fields separately.
        // Optimizing this away would lead to missing data from synthetic source in some cases.
        // We could do both, but we don't do it now.
        assertEquals(1, output.ignoredSourceValues().size());
    }

    public void testSingleElementArray() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "boolean").field("synthetic_source_keep", "arrays"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var doc = new LuceneDocument();

        sut.consume(
            new DocumentParserListener.Event.LeafArrayStart(
                (FieldMapper) mappingLookup.getMapper("field"),
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        sut.consume((DocumentParserListener.Token.ValueToken<Boolean>) () -> true);
        sut.consume(DocumentParserListener.Token.END_ARRAY);

        var output = sut.finish();

        // Since there is only one value in the array, order does not matter,
        // and we can drop ignored source value and use standard synthetic source logic.
        assertEquals(0, output.ignoredSourceValues().size());
    }

    public void testMultipleSingleElementArrays() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        var mapping = fieldMapping(b -> b.field("type", "boolean").field("synthetic_source_keep", "arrays"));
        var mappingLookup = createSytheticSourceMapperService(mapping).mappingLookup();
        var sut = new SyntheticSourceDocumentParserListener(mappingLookup, xContentType);

        var doc = new LuceneDocument();

        sut.consume(
            new DocumentParserListener.Event.LeafArrayStart(
                (FieldMapper) mappingLookup.getMapper("field"),
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        sut.consume((DocumentParserListener.Token.ValueToken<Boolean>) () -> true);
        sut.consume(DocumentParserListener.Token.END_ARRAY);

        sut.consume(
            new DocumentParserListener.Event.LeafArrayStart(
                (FieldMapper) mappingLookup.getMapper("field"),
                mappingLookup.getMapping().getRoot(),
                doc
            )
        );
        sut.consume((DocumentParserListener.Token.ValueToken<Boolean>) () -> false);
        sut.consume(DocumentParserListener.Token.END_ARRAY);

        var output = sut.finish();

        // Since there is only one value in the array, order does not matter,
        // and we can drop ignored source value.
        assertEquals(0, output.ignoredSourceValues().size());
    }
}
