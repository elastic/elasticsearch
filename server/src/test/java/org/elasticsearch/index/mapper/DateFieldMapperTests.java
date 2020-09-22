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
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DateFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "date");
    }

    @Override
    protected void assertParseMaximalWarnings() {
        assertWarnings("Parameter [boost] on field [field] is deprecated and will be removed in 8.0");
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

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

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("index", false)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("doc_values", false)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
    }

    public void testStore() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("store", true)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

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

    public void testIgnoreMalformed() throws IOException {
        testIgnoreMalformedForValue("2016-03-99",
                "failed to parse date field [2016-03-99] with format [strict_date_optional_time||epoch_millis]");
        testIgnoreMalformedForValue("-2147483648",
                "Invalid value for Year (valid values -999999999 - 999999999): -2147483648");
        testIgnoreMalformedForValue("-522000000", "long overflow");
    }

    private void testIgnoreMalformedForValue(String value, String expectedCause) throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", value))));
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [date]"));
        assertThat(e.getMessage(), containsString("Preview of field's value: '" + value + "'"));
        assertThat(e.getCause().getMessage(), containsString(expectedCause));

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("ignore_malformed", true)));

        ParsedDocument doc = mapper2.parse(source(b -> b.field("field", value)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
    }

    public void testChangeFormat() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("format", "epoch_second")));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 1457654400)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1457654400000L, pointField.numericValue().longValue());
    }

    public void testChangeLocale() throws IOException {
        assumeTrue("need java 9 for testing ",JavaVersion.current().compareTo(JavaVersion.parse("9")) >= 0);

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("format", "E, d MMM yyyy HH:mm:ss Z")
            .field("locale", "de")));

        mapper.parse(source(b -> b.field("field", "Mi, 06 Dez 2000 02:55:00 -0800")));
    }

    public void testNullValue() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("null_value", "2016-03-11")));

        doc = mapper.parse(source(b -> b.nullField("field")));
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

    public void testNanosNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        MapperService mapperService = createMapperService(fieldMapping(b -> b
            .field("type", "date_nanos")
            .field("null_value", "2016-03-11")));

        DateFieldMapper.DateFieldType ft = (DateFieldMapper.DateFieldType) mapperService.fieldType("field");
        long expectedNullValue = ft.parse("2016-03-11");

        doc = mapperService.documentMapper().parse(source(b -> b.nullField("field")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(expectedNullValue, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(expectedNullValue, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testBadNullValue() throws IOException {

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> createDocumentMapper(Version.V_8_0_0, fieldMapping(b -> b.field("type", "date").field("null_value", "foo"))));

        assertThat(e.getMessage(),
            equalTo("Failed to parse mapping: Error parsing [null_value] on field [field]: " +
                "failed to parse date field [foo] with format [strict_date_optional_time||epoch_millis]"));

        createDocumentMapper(Version.V_7_9_0, fieldMapping(b -> b.field("type", "date").field("null_value", "foo")));

        assertWarnings("Error parsing [foo] as date in [null_value] on field [field]); [null_value] will be ignored");
    }

    public void testNullConfigValuesFail() {
        Exception e = expectThrows(MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").nullField("format"))));
        assertThat(e.getMessage(), containsString("[format] on mapper [field] of type [date] must not have a [null] value"));
    }

    public void testTimeZoneParsing() throws Exception {
        final String timeZonePattern = "yyyy-MM-dd" + randomFrom("XXX", "[XXX]", "'['XXX']'");

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b
            .field("type", "date")
            .field("format", timeZonePattern)));

        DateFormatter formatter = DateFormatter.forPattern(timeZonePattern);
        final ZoneId randomTimeZone = randomBoolean() ? ZoneId.of(randomFrom("UTC", "CET")) : randomZone();
        final ZonedDateTime randomDate = ZonedDateTime.of(2016, 3, 11, 0, 0, 0, 0, randomTimeZone);

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", formatter.format(randomDate))));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        long millis = randomDate.withZoneSameInstant(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(millis, fields[0].numericValue().longValue());
    }

    public void testMergeDate() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b
            .field("type", "date")
            .field("format", "yyyy/MM/dd")));

        assertThat(mapperService.fieldType("field"), notNullValue());
        assertFalse(mapperService.fieldType("field").isStored());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "date").field("format", "epoch_millis"))));
        assertThat(e.getMessage(), containsString("parameter [format] from [yyyy/MM/dd] to [epoch_millis]"));
    }

    public void testMergeText() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> merge(mapperService, fieldMapping(b -> b.field("type", "text"))));
        assertEquals("mapper [field] cannot be changed from type [date] to [text]", e.getMessage());
    }

    public void testIllegalFormatField() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> b
                    .field("type", "date")
                    .field("format", "test_format"))));
        assertThat(e.getMessage(), containsString("Invalid format: [test_format]: Unknown pattern letter: t"));
        assertThat(e.getMessage(), containsString("Error parsing [format] on field [field]: Invalid"));
    }

    public void testFetchSourceValue() throws IOException {
        DateFieldMapper mapper = createMapper(Resolution.MILLISECONDS, null);
        String date = "2020-05-15T21:33:02.000Z";
        assertEquals(List.of(date), fetchSourceValue(mapper, date));
        assertEquals(List.of(date), fetchSourceValue(mapper, 1589578382000L));

        DateFieldMapper mapperWithFormat = createMapper(Resolution.MILLISECONDS, "yyyy/MM/dd||epoch_millis");
        String dateInFormat = "1990/12/29";
        assertEquals(List.of(dateInFormat), fetchSourceValue(mapperWithFormat, dateInFormat));
        assertEquals(List.of(dateInFormat), fetchSourceValue(mapperWithFormat, 662428800000L));

        DateFieldMapper mapperWithMillis = createMapper(Resolution.MILLISECONDS, "epoch_millis");
        String dateInMillis = "662428800000";
        assertEquals(List.of(dateInMillis), fetchSourceValue(mapperWithMillis, dateInMillis));
        assertEquals(List.of(dateInMillis), fetchSourceValue(mapperWithMillis, 662428800000L));

        String nullValueDate = "2020-05-15T21:33:02.000Z";
        DateFieldMapper nullValueMapper = createMapper(Resolution.MILLISECONDS, null, nullValueDate);
        assertEquals(List.of(nullValueDate), fetchSourceValue(nullValueMapper, null));
    }

    public void testParseSourceValueWithFormat() throws IOException {
        DateFieldMapper mapper = createMapper(Resolution.NANOSECONDS, "strict_date_time", "1970-12-29T00:00:00.000Z");
        String date = "1990-12-29T00:00:00.000Z";
        assertEquals(List.of("1990/12/29"), fetchSourceValue(mapper, date, "yyyy/MM/dd"));
        assertEquals(List.of("662428800000"), fetchSourceValue(mapper, date, "epoch_millis"));
        assertEquals(List.of("1970/12/29"), fetchSourceValue(mapper, null, "yyyy/MM/dd"));
    }

    public void testParseSourceValueNanos() throws IOException {
        DateFieldMapper mapper = createMapper(Resolution.NANOSECONDS, "strict_date_time||epoch_millis");
        String date = "2020-05-15T21:33:02.123456789Z";
        assertEquals(List.of("2020-05-15T21:33:02.123456789Z"), fetchSourceValue(mapper, date));
        assertEquals(List.of("2020-05-15T21:33:02.123Z"), fetchSourceValue(mapper, 1589578382123L));

        String nullValueDate = "2020-05-15T21:33:02.123456789Z";
        DateFieldMapper nullValueMapper = createMapper(Resolution.NANOSECONDS, "strict_date_time||epoch_millis", nullValueDate);
        assertEquals(List.of(nullValueDate), fetchSourceValue(nullValueMapper, null));
    }

    public void testFetchDocValuesMillis() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date").field("format", "strict_date_time||epoch_millis"))
        );
        MappedFieldType ft = mapperService.fieldType("field");
        DocValueFormat format = ft.docValueFormat(null, null);
        String date = "2020-05-15T21:33:02.123Z";
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, date));
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, 1589578382123L));
    }

    public void testFetchDocValuesNanos() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date_nanos").field("format", "strict_date_time||epoch_millis"))
        );
        MappedFieldType ft = mapperService.fieldType("field");
        DocValueFormat format = ft.docValueFormat(null, null);
        String date = "2020-05-15T21:33:02.123456789Z";
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, date));
        assertEquals(List.of("2020-05-15T21:33:02.123Z"), fetchFromDocValues(mapperService, ft, format, 1589578382123L));
    }

    private DateFieldMapper createMapper(Resolution resolution, String format) {
        return createMapper(resolution, format, null);
    }

    private DateFieldMapper createMapper(Resolution resolution, String format, String nullValue) {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "date_nanos");
        if (format != null) {
            mapping.put("format", format);
        }
        if (nullValue != null) {
            mapping.put("null_value", nullValue);
        }

        DateFieldMapper.Builder builder
            = new DateFieldMapper.Builder("field", resolution, null, false, Version.CURRENT);
        builder.parse("field", null, mapping);
        return builder.build(context);
    }
}
