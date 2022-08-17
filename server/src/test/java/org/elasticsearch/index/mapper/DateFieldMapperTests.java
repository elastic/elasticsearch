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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

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
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> assertTrue(((DateFieldMapper) m).getIgnoreMalformed()));
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

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("index", false)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("doc_values", false)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "2016-03-11")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
    }

    public void testStore() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("store", true)));

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
        testIgnoreMalformedForValue(
            "2016-03-99",
            "failed to parse date field [2016-03-99] with format [strict_date_optional_time||epoch_millis]",
            "strict_date_optional_time||epoch_millis"
        );
        testIgnoreMalformedForValue("-522000000", "long overflow", "date_optional_time");
    }

    private void testIgnoreMalformedForValue(String value, String expectedCause, String dateFormat) throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping((builder) -> dateFieldMapping(builder, dateFormat)));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", value))));
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [date]"));
        assertThat(e.getMessage(), containsString("Preview of field's value: '" + value + "'"));
        assertThat(e.getCause().getMessage(), containsString(expectedCause));

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "date").field("format", dateFormat).field("ignore_malformed", true))
        );

        ParsedDocument doc = mapper2.parse(source(b -> b.field("field", value)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
    }

    private void dateFieldMapping(XContentBuilder builder, String dateFormat) throws IOException {
        builder.field("type", "date");
        builder.field("format", dateFormat);

    }

    public void testChangeFormat() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("format", "epoch_second")));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 1457654400)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1457654400000L, pointField.numericValue().longValue());
    }

    public void testChangeLocale() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "date").field("format", "E, d MMM yyyy HH:mm:ss Z").field("locale", "de"))
        );

        mapper.parse(source(b -> b.field("field", "Mi, 06 Dez 2000 02:55:00 -0800")));
    }

    public void testNullValue() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("null_value", "2016-03-11")));

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

        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "date_nanos").field("null_value", "2016-03-11"))
        );

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

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(Version.V_8_0_0, fieldMapping(b -> b.field("type", "date").field("null_value", "foo")))
        );

        assertThat(
            e.getMessage(),
            equalTo(
                "Failed to parse mapping: Error parsing [null_value] on field [field]: "
                    + "failed to parse date field [foo] with format [strict_date_optional_time||epoch_millis]"
            )
        );

        createDocumentMapper(Version.V_7_9_0, fieldMapping(b -> b.field("type", "date").field("null_value", "foo")));

        assertWarnings("Error parsing [foo] as date in [null_value] on field [field]); [null_value] will be ignored");
    }

    public void testNullConfigValuesFail() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").nullField("format")))
        );
        assertThat(e.getMessage(), containsString("[format] on mapper [field] of type [date] must not have a [null] value"));
    }

    public void testTimeZoneParsing() throws Exception {
        final String timeZonePattern = "yyyy-MM-dd" + randomFrom("XXX", "[XXX]", "'['XXX']'");

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("format", timeZonePattern)));

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
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "date").field("format", "yyyy/MM/dd")));

        assertThat(mapperService.fieldType("field"), notNullValue());
        assertFalse(mapperService.fieldType("field").isStored());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "date").field("format", "epoch_millis")))
        );
        assertThat(e.getMessage(), containsString("parameter [format] from [yyyy/MM/dd] to [epoch_millis]"));
    }

    public void testMergeText() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "text")))
        );
        assertEquals("mapper [field] cannot be changed from type [date] to [text]", e.getMessage());
    }

    public void testIllegalFormatField() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "date").field("format", "test_format")))
        );
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

    public void testFormatPreserveNanos() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "date_nanos")));
        DateFieldMapper.DateFieldType ft = (DateFieldMapper.DateFieldType) mapperService.fieldType("field");
        assertEquals(ft.dateTimeFormatter, DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER);
        DocValueFormat format = ft.docValueFormat(null, null);
        String date = "2020-05-15T21:33:02.123456789Z";
        assertEquals(List.of(date), fetchFromDocValues(mapperService, ft, format, date));
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

    /**
     * The max date iso8601 can parse. It'll format much larger dates.
     */
    private static final long MAX_ISO_DATE = DEFAULT_DATE_TIME_FORMATTER.parseMillis("9999-12-12T23:59:59.999Z");

    public void testFetchMillis() throws IOException {
        assertFetch(dateMapperService(), "field", randomLongBetween(0, Long.MAX_VALUE), null);
    }

    public void testFetchMillisFromMillisFormatted() throws IOException {
        assertFetch(dateMapperService(), "field", randomLongBetween(0, Long.MAX_VALUE), "epoch_millis");
    }

    public void testFetchMillisFromMillisFormattedIso8601() throws IOException {
        assertFetch(dateMapperService(), "field", randomLongBetween(0, Long.MAX_VALUE), "iso8601");
    }

    public void testFetchMillisFromIso8601() throws IOException {
        assertFetch(dateMapperService(), "field", DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomLongBetween(0, MAX_ISO_DATE)), "iso8601");
    }

    public void testFetchMillisFromIso8601Nanos() throws IOException {
        assertFetch(dateMapperService(), "field", randomIs8601Nanos(MAX_ISO_DATE), null);
    }

    public void testFetchMillisFromIso8601NanosFormatted() throws IOException {
        assertFetch(dateMapperService(), "field", randomIs8601Nanos(MAX_ISO_DATE), "strict_date_optional_time_nanos");
    }

    /**
     * Tests round tripping a date with nanosecond resolution through doc
     * values and field fetching via the {@code date} field. We expect this to
     * lose precision because the {@code date} field only supports millisecond
     * resolution. But its important that this lose precision in the same
     * way.
     */
    public void testFetchMillisFromRoundedNanos() throws IOException {
        assertFetch(dateMapperService(), "field", randomDecimalNanos(MAX_ISO_DATE), null);
    }

    /**
     * Tests round tripping a date with nanosecond resolution through doc
     * values and field fetching via the {@code date} field with a specific
     * format. We expect this to lose precision because the {@code date}
     * field only supports millisecond resolution. But its important that
     * this lose precision in the same way.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/70085") // Fails about 1/1000 of the time because of rounding.
    public void testFetchMillisFromFixedNanos() throws IOException {
        assertFetch(dateMapperService(), "field", new BigDecimal(randomDecimalNanos(MAX_ISO_DATE)), null);
    }

    private MapperService dateMapperService() throws IOException {
        return createMapperService(mapping(b -> b.startObject("field").field("type", "date").endObject()));
    }

    /**
     * The maximum valid nanosecond date in milliseconds since epoch.
     */
    private static final long MAX_NANOS = DateUtils.MAX_NANOSECOND_INSTANT.toEpochMilli();

    public void testFetchNanos() throws IOException {
        assertFetch(dateNanosMapperService(), "field", randomLongBetween(0, MAX_NANOS), null);
    }

    public void testFetchNanosFromMillisFormatted() throws IOException {
        assertFetch(dateNanosMapperService(), "field", randomLongBetween(0, MAX_NANOS), "epoch_millis");
    }

    public void testFetchNanosFromMillisFormattedIso8601() throws IOException {
        assertFetch(dateNanosMapperService(), "field", randomLongBetween(0, MAX_NANOS), "iso8601");
    }

    public void testFetchNanosFromIso8601Nanos() throws IOException {
        assertFetch(dateNanosMapperService(), "field", randomIs8601Nanos(MAX_NANOS), null);
    }

    public void testFetchNanosFromIso8601NanosFormatted() throws IOException {
        assertFetch(dateNanosMapperService(), "field", randomIs8601Nanos(MAX_NANOS), "strict_date_optional_time_nanos");
    }

    public void testFetchNanosFromRoundedNanos() throws IOException {
        assertFetch(dateNanosMapperService(), "field", randomDecimalNanos(MAX_NANOS), null);
    }

    /**
     * Maximum date we can round trip through {@code date_nanos} without
     * losing precision right now. We hope to be able to make this
     * {@link #MAX_NANOS} soon.
     * <p>
     * Given the maximum precise value for a double (9,007,199,254,740,992)
     * I'd expect this to be 1970-04-15T05:59:59.253Z but that causes
     * errors. I'm curious about why but not curious enough to track it down.
     */
    private static final long MAX_MILLIS_DOUBLE_NANOS_KEEPS_PRECISION = DEFAULT_DATE_TIME_FORMATTER.parseMillis("1970-04-10T00:00:00.000Z");

    /**
     * Tests round tripping a date with nanosecond resolution through doc
     * values and field fetching via the {@code date_nanos} field.
     */
    public void testFetchNanosFromFixedNanos() throws IOException {
        assertFetch(dateNanosMapperService(), "field", new BigDecimal(randomDecimalNanos(MAX_MILLIS_DOUBLE_NANOS_KEEPS_PRECISION)), null);
    }

    /**
     * Tests round tripping a date with nanosecond resolution through doc
     * values and field fetching via the {@code date_nanos} field when there
     * is a format.
     */
    public void testFetchNanosFromFixedNanosFormatted() throws IOException {
        assertFetch(
            dateNanosMapperService(),
            "field",
            new BigDecimal(randomDecimalNanos(MAX_MILLIS_DOUBLE_NANOS_KEEPS_PRECISION)),
            "strict_date_optional_time_nanos"
        );
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", randomBoolean() ? "date" : "date_nanos");
    }

    @Override
    protected String randomFetchTestFormat() {
        // TODO more choices! The test should work fine even for choices that throw out a ton of precision.
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> "epoch_millis";
            case 2 -> "iso8601";
            default -> throw new IllegalStateException();
        };
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        switch (((DateFieldType) ft).resolution()) {
            case MILLISECONDS:
                if (randomBoolean()) {
                    return randomIs8601Nanos(MAX_ISO_DATE);
                }
                return randomLongBetween(0, Long.MAX_VALUE);
            case NANOSECONDS:
                return switch (randomInt(2)) {
                    case 0 -> randomLongBetween(0, MAX_NANOS);
                    case 1 -> randomIs8601Nanos(MAX_NANOS);
                    case 2 -> new BigDecimal(randomDecimalNanos(MAX_MILLIS_DOUBLE_NANOS_KEEPS_PRECISION));
                    default -> throw new IllegalStateException();
                };
            default:
                throw new IllegalStateException();
        }
    }

    private MapperService dateNanosMapperService() throws IOException {
        return createMapperService(mapping(b -> b.startObject("field").field("type", "date_nanos").endObject()));
    }

    private String randomIs8601Nanos(long maxMillis) {
        String date = DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.formatMillis(randomLongBetween(0, maxMillis));
        date = date.substring(0, date.length() - 1);  // Strip off trailing "Z"
        return date + String.format(Locale.ROOT, "%06d", between(0, 999999)) + "Z";  // Add nanos and the "Z"
    }

    private String randomDecimalNanos(long maxMillis) {
        return Long.toString(randomLongBetween(0, maxMillis)) + "." + between(0, 999999);
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "date");
                b.field("script", "test");
                b.field("null_value", 7);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "date");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport() {
            private final DateFieldMapper.Resolution resolution = randomFrom(DateFieldMapper.Resolution.values());
            private final Object nullValue = usually()
                ? null
                : randomValueOtherThanMany(
                    v -> v instanceof BigDecimal,  // BigDecimal values don't parse properly so limit the test to others
                    () -> randomValue()
                );
            private final DateFormatter formatter = resolution == DateFieldMapper.Resolution.MILLISECONDS
                ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER
                : DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER;

            @Override
            public SyntheticSourceExample example(int maxValues) {
                if (randomBoolean()) {
                    Tuple<Object, String> v = generateValue();
                    return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
                }
                List<Tuple<Object, String>> values = randomList(1, maxValues, this::generateValue);
                List<Object> in = values.stream().map(Tuple::v1).toList();
                List<String> outList = values.stream()
                    .sorted(
                        Comparator.comparing(v -> Instant.from(formatter.parse(v.v1() == null ? nullValue.toString() : v.v1().toString())))
                    )
                    .map(Tuple::v2)
                    .toList();
                Object out = outList.size() == 1 ? outList.get(0) : outList;
                return new SyntheticSourceExample(in, out, this::mapping);
            }

            private Tuple<Object, String> generateValue() {
                if (nullValue != null && randomBoolean()) {
                    return Tuple.tuple(null, outValue(nullValue));
                }
                Object in = randomValue();
                String out = outValue(in);
                return Tuple.tuple(in, out);
            }

            private Object randomValue() {
                switch (resolution) {
                    case MILLISECONDS:
                        if (randomBoolean()) {
                            return randomIs8601Nanos(MAX_ISO_DATE);
                        }
                        return randomLongBetween(0, MAX_ISO_DATE);
                    case NANOSECONDS:
                        return switch (randomInt(2)) {
                            case 0 -> randomLongBetween(0, MAX_NANOS);
                            case 1 -> randomIs8601Nanos(MAX_NANOS);
                            case 2 -> new BigDecimal(randomDecimalNanos(MAX_MILLIS_DOUBLE_NANOS_KEEPS_PRECISION));
                            default -> throw new IllegalStateException();
                        };
                    default:
                        throw new IllegalStateException();
                }
            }

            private String outValue(Object in) {
                return formatter.format(formatter.parse(in.toString()));
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", resolution.type());
                if (nullValue != null) {
                    b.field("null_value", nullValue);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                List<SyntheticSourceInvalidExample> examples = new ArrayList<>();
                for (String fieldType : new String[] { "date", "date_nanos" }) {
                    examples.add(
                        new SyntheticSourceInvalidExample(
                            equalTo(
                                "field [field] of type ["
                                    + fieldType
                                    + "] doesn't support synthetic source because it doesn't have doc values"
                            ),
                            b -> b.field("type", fieldType).field("doc_values", false)
                        )
                    );
                    examples.add(
                        new SyntheticSourceInvalidExample(
                            equalTo(
                                "field [field] of type ["
                                    + fieldType
                                    + "] doesn't support synthetic source because it ignores malformed dates"
                            ),
                            b -> b.field("type", fieldType).field("ignore_malformed", true)
                        )
                    );
                }
                return examples;
            }
        };
    }

    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            protected DateFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, formatter) -> ctx -> new DateFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    formatter,
                    ctx
                ) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected DateFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, formatter) -> ctx -> new DateFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    formatter,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit(1649343081000L);
                    }
                };
            }
        };
    }

    public void testLegacyField() throws Exception {
        // check that unknown date formats are treated leniently on old indices
        MapperService service = createMapperService(Version.fromString("5.0.0"), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("mydate");
            b.field("type", "date");
            b.field("format", "unknown-format");
            b.endObject();
        }));
        assertThat(service.fieldType("mydate"), instanceOf(DateFieldType.class));
        assertEquals(DEFAULT_DATE_TIME_FORMATTER, ((DateFieldType) service.fieldType("mydate")).dateTimeFormatter);

        // check that date format can be updated
        merge(service, mapping(b -> {
            b.startObject("mydate");
            b.field("type", "date");
            b.field("format", "YYYY/MM/dd");
            b.endObject();
        }));
        assertThat(service.fieldType("mydate"), instanceOf(DateFieldType.class));
        assertNotEquals(DEFAULT_DATE_TIME_FORMATTER, ((DateFieldType) service.fieldType("mydate")).dateTimeFormatter);
    }

    public void testLegacyDateFormatName() {
        DateFieldMapper.Builder builder = new DateFieldMapper.Builder(
            "format",
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            mock(ScriptService.class),
            true,
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_8_0_0)) // BWC compatible
                                                                                                                           // index, e.g 7.x
        );

        // Check that we allow the use of camel case date formats on 7.x indices
        @SuppressWarnings("unchecked")
        FieldMapper.Parameter<String> formatParam = (FieldMapper.Parameter<String>) builder.getParameters()[3];
        formatParam.parse("date_time_format", mock(MappingParserContext.class), "strictDateOptionalTime");
        builder.buildFormatter(); // shouldn't throw exception

        formatParam.parse("date_time_format", mock(MappingParserContext.class), "strictDateOptionalTime||strictDateOptionalTimeNanos");
        builder.buildFormatter(); // shouldn't throw exception

        DateFieldMapper.Builder newFieldBuilder = new DateFieldMapper.Builder(
            "format",
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            mock(ScriptService.class),
            true,
            Version.CURRENT
        );

        @SuppressWarnings("unchecked")
        final FieldMapper.Parameter<String> newFormatParam = (FieldMapper.Parameter<String>) newFieldBuilder.getParameters()[3];

        // Check that we don't allow the use of camel case date formats on 8.x indices
        assertEquals(
            "Error parsing [format] on field [format]: Invalid format: [strictDateOptionalTime]: Unknown pattern letter: t",
            expectThrows(IllegalArgumentException.class, () -> {
                newFormatParam.parse("date_time_format", mock(MappingParserContext.class), "strictDateOptionalTime");
                assertEquals("strictDateOptionalTime", newFormatParam.getValue());
                newFieldBuilder.buildFormatter();
            }).getMessage()
        );

    }
}
