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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class DateFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("index", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("doc_values", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
    }

    public void testStore() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("store", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1457654400000L, storedField.numericValue().longValue());
    }

    public void testIgnoreMalformed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-99")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("Cannot parse \"2016-03-99\""));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date")
                .field("ignore_malformed", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper2.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", ":1")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, doc.rootDoc().getValues("_ignored"));
    }

    public void testChangeFormat() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date")
                .field("format", "epoch_second").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 1457654400)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1457654400000L, pointField.numericValue().longValue());
    }

    public void testFloatEpochFormat() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date")
                .field("format", "epoch_millis").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        long epochMillis = randomNonNegativeLong();
        String epochFloatValue = epochMillis + "." + randomIntBetween(0, 999);

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", epochFloatValue)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(epochMillis, pointField.numericValue().longValue());
    }

    public void testChangeLocale() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("locale", "fr").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 1457654400)
                        .endObject()),
                XContentType.JSON));
    }

    public void testNullValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .field("null_value", "2016-03-11")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNullConfigValuesFail() throws MapperParsingException, IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .field("format", (String) null)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        Exception e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[format] must not have a [null] value", e.getMessage());
    }

    public void testEmptyName() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "date")
            .field("format", "epoch_second").endObject().endObject()
            .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    /**
     * Test that time zones are correctly parsed by the {@link DateFieldMapper}.
     * There is a known bug with Joda 2.9.4 reported in https://github.com/JodaOrg/joda-time/issues/373.
     */
    public void testTimeZoneParsing() throws Exception {
        final String timeZonePattern = "yyyy-MM-dd" + randomFrom("ZZZ", "[ZZZ]", "'['ZZZ']'");

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .field("format", timeZonePattern)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        final DateTimeZone randomTimeZone = randomBoolean() ? DateTimeZone.forID(randomFrom("UTC", "CET")) : randomDateTimeZone();
        final DateTime randomDate = new DateTime(2016, 03, 11, 0, 0, 0, randomTimeZone);

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("field", DateTimeFormat.forPattern(timeZonePattern).print(randomDate))
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(randomDate.withZone(DateTimeZone.UTC).getMillis(), fields[0].numericValue().longValue());
    }

    public void testMergeDate() throws IOException {
        String initMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("movie")
            .startObject("properties")
            .startObject("release_date").field("type", "date").field("format", "yyyy/MM/dd").endObject()
            .endObject().endObject().endObject());
        indexService.mapperService().merge("movie", new CompressedXContent(initMapping),
            MapperService.MergeReason.MAPPING_UPDATE);

        assertThat(indexService.mapperService().fullName("release_date"), notNullValue());
        assertFalse(indexService.mapperService().fullName("release_date").stored());

        String updateFormatMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("movie")
            .startObject("properties")
            .startObject("release_date").field("type", "date").field("format", "epoch_millis").endObject()
            .endObject().endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("movie", new CompressedXContent(updateFormatMapping),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("[mapper [release_date] has different [format] values]"));
    }

    public void testMergeText() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("date").field("type", "date").endObject()
                .endObject().endObject().endObject());
        DocumentMapper mapper = indexService.mapperService().parse("_doc", new CompressedXContent(mapping), false);

        String mappingUpdate = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("date").field("type", "text").endObject()
                .endObject().endObject().endObject());
        DocumentMapper update = indexService.mapperService().parse("_doc", new CompressedXContent(mappingUpdate), false);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapper.merge(update.mapping()));
        assertEquals("mapper [date] of different type, current_type [date], merged_type [text]", e.getMessage());
    }

    public void testIllegalFormatField() throws Exception {
        String mapping =  Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .array("format", "test_format")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Invalid format: [[test_format]]: expected string value", e.getMessage());
    }
}
