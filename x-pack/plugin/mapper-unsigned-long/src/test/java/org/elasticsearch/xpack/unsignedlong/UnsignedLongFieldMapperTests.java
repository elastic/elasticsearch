/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapperTestCase;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class UnsignedLongFieldMapperTests extends FieldMapperTestCase<UnsignedLongFieldMapper.Builder> {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(UnsignedLongMapperPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity");
    }

    @Override
    protected UnsignedLongFieldMapper.Builder newBuilder() {
        return new UnsignedLongFieldMapper.Builder("my_unsigned_long");
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_unsigned_long")
                .field("type", "unsigned_long")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        // test that indexing values as string
        {
            ParsedDocument doc = mapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field("my_unsigned_long", "18446744073709551615").endObject()
                    ),
                    XContentType.JSON
                )
            );
            IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
            assertEquals(2, fields.length);
            IndexableField pointField = fields[0];
            assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
            assertFalse(pointField.fieldType().stored());
            assertEquals(9223372036854775807L, pointField.numericValue().longValue());
            IndexableField dvField = fields[1];
            assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
            assertEquals(9223372036854775807L, dvField.numericValue().longValue());
            assertFalse(dvField.fieldType().stored());
        }

        // test indexing values as integer numbers
        {
            ParsedDocument doc = mapper.parse(
                new SourceToParse(
                    "test",
                    "2",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field("my_unsigned_long", 9223372036854775807L).endObject()
                    ),
                    XContentType.JSON
                )
            );
            IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
            assertEquals(2, fields.length);
            IndexableField pointField = fields[0];
            assertEquals(-1L, pointField.numericValue().longValue());
            IndexableField dvField = fields[1];
            assertEquals(-1L, dvField.numericValue().longValue());
        }

        // test that indexing values as number with decimal is not allowed
        {
            ThrowingRunnable runnable = () -> mapper.parse(
                new SourceToParse(
                    "test",
                    "3",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("my_unsigned_long", 10.5).endObject()),
                    XContentType.JSON
                )
            );
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("For input string: [10.5]"));
        }
    }

    public void testNotIndexed() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_unsigned_long")
                .field("type", "unsigned_long")
                .field("index", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("my_unsigned_long", "18446744073709551615").endObject()
                ),
                XContentType.JSON
            )
        );
        IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(9223372036854775807L, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_unsigned_long")
                .field("type", "unsigned_long")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("my_unsigned_long", "18446744073709551615").endObject()
                ),
                XContentType.JSON
            )
        );
        IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(9223372036854775807L, pointField.numericValue().longValue());
    }

    public void testStore() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_unsigned_long")
                .field("type", "unsigned_long")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("my_unsigned_long", "18446744073709551615").endObject()
                ),
                XContentType.JSON
            )
        );
        IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(9223372036854775807L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(9223372036854775807L, dvField.numericValue().longValue());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(9223372036854775807L, storedField.numericValue().longValue());
    }

    public void testCoerceMappingParameterIsIllegal() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_unsigned_long")
                .field("type", "unsigned_long")
                .field("coerce", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        ThrowingRunnable runnable = () -> parser.parse("_doc", new CompressedXContent(mapping));
        ;
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertEquals(e.getMessage(), "Mapping definition for [my_unsigned_long] has unsupported parameters:  [coerce : false]");
    }

    public void testNullValue() throws IOException {
        // test that if null value is not defined, field is not indexed
        {
            String mapping = Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("my_unsigned_long")
                    .field("type", "unsigned_long")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );
            DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
            assertEquals(mapping, mapper.mappingSource().toString());

            ParsedDocument doc = mapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField("my_unsigned_long").endObject()),
                    XContentType.JSON
                )
            );
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("my_unsigned_long"));
        }

        // test that if null value is defined, it is used
        {
            String mapping = Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("my_unsigned_long")
                    .field("type", "unsigned_long")
                    .field("null_value", "18446744073709551615")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );
            DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
            assertEquals(mapping, mapper.mappingSource().toString());

            ParsedDocument doc = mapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField("my_unsigned_long").endObject()),
                    XContentType.JSON
                )
            );
            IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
            assertEquals(2, fields.length);
            IndexableField pointField = fields[0];
            assertEquals(9223372036854775807L, pointField.numericValue().longValue());
            IndexableField dvField = fields[1];
            assertEquals(9223372036854775807L, dvField.numericValue().longValue());
        }
    }

    public void testIgnoreMalformed() throws Exception {
        // test ignore_malformed is false by default
        {
            String mapping = Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("my_unsigned_long")
                    .field("type", "unsigned_long")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );
            DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
            assertEquals(mapping, mapper.mappingSource().toString());

            Object malformedValue1 = "a";
            ThrowingRunnable runnable = () -> mapper.parse(
                new SourceToParse(
                    "test",
                    "_doc",
                    BytesReference.bytes(jsonBuilder().startObject().field("my_unsigned_long", malformedValue1).endObject()),
                    XContentType.JSON
                )
            );
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));

            Object malformedValue2 = Boolean.FALSE;
            runnable = () -> mapper.parse(
                new SourceToParse(
                    "test",
                    "_doc",
                    BytesReference.bytes(jsonBuilder().startObject().field("my_unsigned_long", malformedValue2).endObject()),
                    XContentType.JSON
                )
            );
            e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Current token"));
            assertThat(e.getCause().getMessage(), containsString("not numeric, can not use numeric value accessors"));
        }

        // test ignore_malformed when set to true ignored malformed documents
        {
            String mapping = Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("my_unsigned_long")
                    .field("type", "unsigned_long")
                    .field("ignore_malformed", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );
            DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
            assertEquals(mapping, mapper.mappingSource().toString());

            Object malformedValue1 = "a";
            ParsedDocument doc = mapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(jsonBuilder().startObject().field("my_unsigned_long", malformedValue1).endObject()),
                    XContentType.JSON
                )
            );
            IndexableField[] fields = doc.rootDoc().getFields("my_unsigned_long");
            assertEquals(0, fields.length);
            assertArrayEquals(new String[] { "my_unsigned_long" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));

            Object malformedValue2 = Boolean.FALSE;
            ParsedDocument doc2 = mapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(jsonBuilder().startObject().field("my_unsigned_long", malformedValue2).endObject()),
                    XContentType.JSON
                )
            );
            IndexableField[] fields2 = doc2.rootDoc().getFields("my_unsigned_long");
            assertEquals(0, fields2.length);
            assertArrayEquals(new String[] { "my_unsigned_long" }, TermVectorsService.getValues(doc2.rootDoc().getFields("_ignored")));
        }
    }

    public void testIndexingOutOfRangeValues() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_unsigned_long")
                .field("type", "unsigned_long")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));

        for (Object outOfRangeValue : new Object[] { "-1", -1L, "18446744073709551616", new BigInteger("18446744073709551616") }) {
            ThrowingRunnable runnable = () -> mapper.parse(
                new SourceToParse(
                    "test",
                    "_doc",
                    BytesReference.bytes(jsonBuilder().startObject().field("my_unsigned_long", outOfRangeValue).endObject()),
                    XContentType.JSON
                )
            );
            expectThrows(MapperParsingException.class, runnable);
        }
    }
}
