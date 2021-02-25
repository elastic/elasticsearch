/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class DateFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return "2016-03-11";
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "date");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("format", b -> b.field("format", "yyyy-MM-dd"));
        checker.registerConflictCheck("locale", b -> b.field("locale", "es"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "34500000"));
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true),
            m -> assertTrue(((DateFieldMapper)m).getIgnoreMalformed()));
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
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

    public void testResolutionRounding() {
        final long millis = randomLong();
        assertThat(DateFieldMapper.Resolution.MILLISECONDS.roundDownToMillis(millis), equalTo(millis));
        assertThat(DateFieldMapper.Resolution.MILLISECONDS.roundUpToMillis(millis), equalTo(millis));

        final long nanos = randomNonNegativeLong();
        final long down = DateFieldMapper.Resolution.NANOSECONDS.roundDownToMillis(nanos);
        assertThat(DateUtils.toNanoSeconds(down), lessThanOrEqualTo(nanos));
        try {
            assertThat(DateUtils.toNanoSeconds(down + 1), greaterThan(nanos));
        } catch (IllegalArgumentException e) {
            // ok, down+1 was out of range
        }

        final long up = DateFieldMapper.Resolution.NANOSECONDS.roundUpToMillis(nanos);
        try {
            assertThat(DateUtils.toNanoSeconds(up), greaterThanOrEqualTo(nanos));
        } catch (IllegalArgumentException e) {
            // ok, up may be out of range by 1; we check that up-1 is in range below (as long as it's >0)
            assertThat(up, greaterThan(0L));
        }

        if (up > 0) {
            assertThat(DateUtils.toNanoSeconds(up - 1), lessThan(nanos));
        } else {
            assertThat(up, equalTo(0L));
        }
    }
}
