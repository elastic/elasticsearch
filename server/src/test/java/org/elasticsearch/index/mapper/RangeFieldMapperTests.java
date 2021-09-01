/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LT_FIELD;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;


public class RangeFieldMapperTests extends AbstractNumericFieldMapperTestCase {
    private static final String FROM_DATE = "2016-10-31";
    private static final String TO_DATE = "2016-11-01 20:00:00";
    private static final String FROM_IP = "::ffff:c0a8:107";
    private static final String TO_IP = "2001:db8::";
    private static final int FROM = 5;
    private static final String FROM_STR = FROM + "";
    private static final int TO = 10;
    private static final String TO_STR = TO + "";
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";

    @Override
    protected Set<String> types() {
        return Set.of("date_range", "ip_range", "float_range", "double_range", "integer_range", "long_range");
    }

    @Override
    protected Set<String> wholeTypes() {
        return Set.of("integer_range", "long_range");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long_range");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of(getFromField(), getFrom("long_range"), getToField(), getTo("long_range"));
    }

    @Override
    protected Object getSampleValueForQuery() {
        return 6;
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerUpdateCheck(b -> b.field("coerce", false),
            m -> assertFalse(((RangeFieldMapper)m).coerce()));
    }

    private Object getFrom(String type) {
        if (type.equals("date_range")) {
            return FROM_DATE;
        } else if (type.equals("ip_range")) {
            return FROM_IP;
        }
        return random().nextBoolean() ? FROM : FROM_STR;
    }

    private String getFromField() {
        return random().nextBoolean() ? GT_FIELD.getPreferredName() : GTE_FIELD.getPreferredName();
    }

    private String getToField() {
        return random().nextBoolean() ? LT_FIELD.getPreferredName() : LTE_FIELD.getPreferredName();
    }

    private Object getTo(String type) {
        if (type.equals("date_range")) {
            return TO_DATE;
        } else if (type.equals("ip_range")) {
            return TO_IP;
        }
        return random().nextBoolean() ? TO : TO_STR;
    }

    private Object getMax(String type) {
        if (type.equals("date_range") || type.equals("long_range")) {
            return Long.MAX_VALUE;
        } else if (type.equals("ip_range")) {
            return InetAddressPoint.MAX_VALUE;
        } else if (type.equals("integer_range")) {
            return Integer.MAX_VALUE;
        } else if (type.equals("float_range")) {
            return Float.POSITIVE_INFINITY;
        }
        return Double.POSITIVE_INFINITY;
    }

    private XContentBuilder rangeFieldMapping(String type, CheckedConsumer<XContentBuilder, IOException> extra) throws IOException {
        return fieldMapping(b -> {
            b.field("type", type);
            if (type.contentEquals("date_range")) {
                b.field("format", DATE_FORMAT);
            }
            extra.accept(b);
        });
    }

    @Override
    public void doTestDefaults(String type) throws Exception {
        XContentBuilder mapping = rangeFieldMapping(type, b -> {});
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field(getFromField(), getFrom(type)).field(getToField(), getTo(type)).endObject())
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());

        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
    }

    @Override
    protected void doTestNotIndexed(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> b.field("index", false)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field(getFromField(), getFrom(type)).field(getToField(), getTo(type)).endObject())
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
    }

    @Override
    protected void doTestNoDocValues(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> b.field("doc_values", false)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field(getFromField(), getFrom(type)).field(getToField(), getTo(type)).endObject())
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
    }

    @Override
    protected void doTestStore(String type) throws Exception {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> b.field("store", true)));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field(getFromField(), getFrom(type)).field(getToField(), getTo(type)).endObject())
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        String strVal = "5";
        if (type.equals("date_range")) {
            strVal = "1477872000000";
        } else if (type.equals("ip_range")) {
            strVal = InetAddresses.toAddrString(InetAddresses.forString("192.168.1.7")) + " : "
                + InetAddresses.toAddrString(InetAddresses.forString("2001:db8:0:0:0:0:0:0"));
        }
        assertThat(storedField.stringValue(), containsString(strVal));
    }

    @Override
    public void doTestCoerce(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> {}));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field(getFromField(), getFrom(type)).field(getToField(), getTo(type)).endObject())
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());

        // date_range ignores the coerce parameter and epoch_millis date format truncates floats (see issue: #14641)
        if (type.equals("date_range") == false) {
            DocumentMapper mapper2 = createDocumentMapper(rangeFieldMapping(type, b -> b.field("coerce", false)));

            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper2.parse(source(b -> b.startObject("field").field(getFromField(), "5.2").field(getToField(), "10").endObject()))
            );
            assertThat(e.getCause().getMessage(), anyOf(containsString("passed as String"), containsString("failed to parse date"),
                    containsString("is not an IP string literal")));
        }
    }

    @Override
    protected void doTestDecimalCoerce(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> {}));

        ParsedDocument doc1 = mapper.parse(
            source(
                b -> b.startObject("field")
                    .field(GT_FIELD.getPreferredName(), "2.34")
                    .field(LT_FIELD.getPreferredName(), "5.67")
                    .endObject()
            )
        );

        ParsedDocument doc2 = mapper.parse(
            source(b -> b.startObject("field").field(GT_FIELD.getPreferredName(), "2").field(LT_FIELD.getPreferredName(), "5").endObject())
        );

        IndexableField[] fields1 = doc1.rootDoc().getFields("field");
        IndexableField[] fields2 = doc2.rootDoc().getFields("field");

        assertEquals(fields1[1].binaryValue(), fields2[1].binaryValue());
    }

    @Override
    protected void doTestNullValue(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> b.field("store", true)));

        // test null value for min and max
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").nullField(getFromField()).nullField(getToField()).endObject())
        );
        assertEquals(3, doc.rootDoc().getFields("field").length);
        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField storedField = fields[2];
        String expected = type.equals("ip_range") ? InetAddresses.toAddrString((InetAddress)getMax(type)) : getMax(type) +"";
        assertThat(storedField.stringValue(), containsString(expected));

        // test null max value
        doc = mapper.parse(source(b -> b.startObject("field").field(getFromField(), getFrom(type)).nullField(getToField()).endObject()));
        fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        String strVal = "5";
        if (type.equals("date_range")) {
            strVal = "1477872000000";
        } else if (type.equals("ip_range")) {
            strVal = InetAddresses.toAddrString(InetAddresses.forString("192.168.1.7")) + " : "
                + InetAddresses.toAddrString(InetAddresses.forString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        }
        assertThat(storedField.stringValue(), containsString(strVal));

        // test null range
        doc = mapper.parse(source(b -> b.nullField("field")));
        assertNull(doc.rootDoc().get("field"));
    }

    public void testNoBounds() throws Exception {
        for (String type : types()) {
            doTestNoBounds(type);
        }
    }

    public void doTestNoBounds(String type) throws IOException {
        DocumentMapper mapper = createDocumentMapper(rangeFieldMapping(type, b -> b.field("store", true)));

        // test no bounds specified
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field").endObject()));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        String expected = type.equals("ip_range") ? InetAddresses.toAddrString((InetAddress)getMax(type)) : getMax(type) +"";
        assertThat(storedField.stringValue(), containsString(expected));
    }

    public void testIllegalArguments() throws Exception {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", RangeType.INTEGER.name).field("format", DATE_FORMAT)))
        );
        assertThat(e.getMessage(), containsString("should not define a dateTimeFormatter"));
    }

    public void testSerializeDefaults() throws Exception {
        for (String type : types()) {
            DocumentMapper docMapper = createDocumentMapper(fieldMapping(b -> b.field("type", type)));
            RangeFieldMapper mapper = (RangeFieldMapper) docMapper.mapping().getRoot().getMapper("field");
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
            mapper.doXContentBody(builder, params);
            String got = Strings.toString(builder.endObject());

            // if type is date_range we check that the mapper contains the default format and locale
            // otherwise it should not contain a locale or format
            assertTrue(got, got.contains("\"format\":\"strict_date_optional_time||epoch_millis\"") == type.equals("date_range"));
            assertTrue(got, got.contains("\"locale\":" + "\"" + Locale.ROOT + "\"") == type.equals("date_range"));
        }
    }

    public void testIllegalFormatField() throws Exception {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "date_range").array("format", "test_format")))
        );
        assertThat(e.getMessage(), containsString("Invalid format: [[test_format]]: Unknown pattern letter: t"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        // Doc value fetching crashes.
        // https://github.com/elastic/elasticsearch/issues/70269
        // TODO when we fix doc values fetcher we should add tests for date and ip ranges.
        assumeFalse("DocValuesFetcher doesn't work", true);
        return null;
    }
}
