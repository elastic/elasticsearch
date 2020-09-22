/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;
import org.elasticsearch.index.termvectors.TermVectorsService;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;

public class NumberFieldMapperTests extends AbstractNumericFieldMapperTestCase {

    @Override
    protected Set<String> types() {
        return Set.of("byte", "short", "integer", "long", "float", "double", "half_float");
    }

    @Override
    protected Set<String> wholeTypes() {
        return Set.of("byte", "short", "integer", "long");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long");
    }

    @Override
    public void doTestDefaults(String type) throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", type));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    @Override
    public void doTestNotIndexed(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    @Override
    public void doTestNoDocValues(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestStore(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(123, storedField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestCoerce(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "123")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("coerce", false)));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper2.parse(source(b -> b.field("field", "123"))));
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    @Override
    protected void doTestDecimalCoerce(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "7.89")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField pointField = fields[0];
        assertEquals(7, pointField.numericValue().doubleValue(), 0d);
    }

    public void testIgnoreMalformed() throws Exception {
        for (String type : types()) {
            DocumentMapper notIgnoring = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
            DocumentMapper ignoring = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("ignore_malformed", true)));
            for (Object malformedValue : new Object[] { "a", Boolean.FALSE }) {
                SourceToParse source = source(b -> b.field("field", malformedValue));
                MapperParsingException e = expectThrows(MapperParsingException.class, () -> notIgnoring.parse(source));
                if (malformedValue instanceof String) {
                    assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));
                } else {
                    assertThat(e.getCause().getMessage(), containsString("Current token"));
                    assertThat(e.getCause().getMessage(), containsString("not numeric, can not use numeric value accessors"));
                }

                ParsedDocument doc = ignoring.parse(source);
                IndexableField[] fields = doc.rootDoc().getFields("field");
                assertEquals(0, fields.length);
                assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
            }
        }
    }

    /**
     * Test that in case the malformed value is an xContent object we throw error regardless of `ignore_malformed`
     */
    public void testIgnoreMalformedWithObject() throws Exception {
        SourceToParse malformed = source(b -> b.startObject("field").field("foo", "bar").endObject());
        for (String type : types()) {
            for (Boolean ignoreMalformed : new Boolean[] { true, false }) {
                DocumentMapper mapper = createDocumentMapper(
                    fieldMapping(b -> b.field("type", type).field("ignore_malformed", ignoreMalformed))
                );
                MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(malformed));
                assertThat(e.getCause().getMessage(), containsString("Current token"));
                assertThat(e.getCause().getMessage(), containsString("not numeric, can not use numeric value accessors"));
            }
        }
    }

    @Override
    protected void doTestNullValue(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
        SourceToParse source = source(b -> b.nullField("field"));
        ParsedDocument doc = mapper.parse(source);
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        Object missing = Arrays.asList("float", "double", "half_float").contains(type) ? 123d : 123L;
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).field("null_value", missing)));
        doc = mapper.parse(source);
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        NumberFieldMapper mapper = new NumberFieldMapper.Builder("field", NumberType.INTEGER, false, true).build(context);
        assertEquals(List.of(3), fetchSourceValue(mapper, 3.14));
        assertEquals(List.of(42), fetchSourceValue(mapper, "42.9"));

        NumberFieldMapper nullValueMapper = new NumberFieldMapper.Builder("field", NumberType.FLOAT, false, true)
            .nullValue(2.71f)
            .build(context);
        assertEquals(List.of(2.71f), fetchSourceValue(nullValueMapper, ""));
        assertEquals(List.of(2.71f), fetchSourceValue(nullValueMapper, null));
    }

    public void testOutOfRangeValues() throws IOException {
        final List<OutOfRangeSpec> inputs = Arrays.asList(
            OutOfRangeSpec.of(NumberType.BYTE, "128", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, "32768", "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.INTEGER, "2147483648", "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.LONG, "9223372036854775808", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.LONG, "1e999999999", "out of range for a long"),

            OutOfRangeSpec.of(NumberType.BYTE, "-129", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, "-32769", "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.INTEGER, "-2147483649", "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.LONG, "-9223372036854775809", "out of range for a long"),
            OutOfRangeSpec.of(NumberType.LONG, "-1e999999999", "out of range for a long"),

            OutOfRangeSpec.of(NumberType.BYTE, 128, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, 32768, "out of range of Java short"),
            OutOfRangeSpec.of(NumberType.INTEGER, 2147483648L, " out of range of int"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("9223372036854775808"), "out of range of long"),

            OutOfRangeSpec.of(NumberType.BYTE, -129, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, -32769, "out of range of Java short"),
            OutOfRangeSpec.of(NumberType.INTEGER, -2147483649L, " out of range of int"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("-9223372036854775809"), "out of range of long"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "-65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "-3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "-1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NaN, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NaN, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NaN, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.POSITIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.POSITIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.POSITIVE_INFINITY, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NEGATIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NEGATIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NEGATIVE_INFINITY, "[double] supports only finite values")
        );

        for(OutOfRangeSpec item: inputs) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", item.type.typeName())));
            try {
                mapper.parse(source(item::write));
                fail("Mapper parsing exception expected for [" + item.type + "] with value [" + item.value + "]");
            } catch (MapperParsingException e) {
                assertThat("Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                    e.getCause().getMessage(), containsString(item.message));
            }
        }

        // the following two strings are in-range for a long after coercion
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "9223372036854775807.9")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(2));
        doc = mapper.parse(source(b -> b.field("field", "-9223372036854775808.9")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(2));
    }

    public void testLongIndexingOutOfRange() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long").field("ignore_malformed", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.rawField("field", new BytesArray("9223372036854775808").streamInput(), XContentType.JSON))
        );
        assertEquals(0, doc.rootDoc().getFields("field").length);
    }
}
