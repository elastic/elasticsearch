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

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;

import static org.elasticsearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LT_FIELD;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class RangeFieldMapperTests extends AbstractNumericFieldMapperTestCase<RangeFieldMapper.Builder> {

    @Override
    protected RangeFieldMapper.Builder newBuilder() {
        return new RangeFieldMapper.Builder("range", RangeType.DATE)
            .format("iso8601");
    }

    @Before
    public void addModifiers() {
        addModifier("format", true, (a, b) -> {
            a.format("basic_week_date");
            b.format("strict_week_date");
        });
        addModifier("locale", true, (a, b) -> {
            a.locale(Locale.CANADA);
            b.locale(Locale.JAPAN);
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    private static String FROM_DATE = "2016-10-31";
    private static String TO_DATE = "2016-11-01 20:00:00";
    private static String FROM_IP = "::ffff:c0a8:107";
    private static String TO_IP = "2001:db8::";
    private static int FROM = 5;
    private static String FROM_STR = FROM + "";
    private static int TO = 10;
    private static String TO_STR = TO + "";
    private static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";

    @Override
    protected void setTypeList() {
        TYPES = new HashSet<>(Arrays.asList("date_range", "ip_range", "float_range", "double_range", "integer_range", "long_range"));
        WHOLE_TYPES = new HashSet<>(Arrays.asList("integer_range", "long_range"));
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

    @Override
    public void doTestDefaults(String type) throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(getFromField(), getFrom(type))
            .field(getToField(), getTo(type))
            .endObject()
            .endObject()),
            XContentType.JSON));

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type).field("index", false);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(getFromField(), getFrom(type))
            .field(getToField(), getTo(type))
            .endObject()
            .endObject()),
            XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
    }

    @Override
    protected void doTestNoDocValues(String type) throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type).field("doc_values", false);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(getFromField(), getFrom(type))
            .field(getToField(), getTo(type))
            .endObject()
            .endObject()),
            XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
    }

    @Override
    protected void doTestStore(String type) throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type).field("store", true);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(getFromField(), getFrom(type))
            .field(getToField(), getTo(type))
            .endObject()
            .endObject()),
            XContentType.JSON));

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));

        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(getFromField(), getFrom(type))
            .field(getToField(), getTo(type))
            .endObject()
            .endObject()),
            XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields[1];
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());

        // date_range ignores the coerce parameter and epoch_millis date format truncates floats (see issue: #14641)
        if (type.equals("date_range") == false) {

            mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("field")
                    .field("type", type).field("coerce", false).endObject().endObject().endObject().endObject();
            DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));

            assertEquals(Strings.toString(mapping), mapper2.mappingSource().toString());

            ThrowingRunnable runnable = () -> mapper2
                    .parse(new SourceToParse(
                            "test", "1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().startObject("field")
                                    .field(getFromField(), "5.2").field(getToField(), "10").endObject().endObject()),
                            XContentType.JSON));
            MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), anyOf(containsString("passed as String"), containsString("failed to parse date"),
                    containsString("is not an IP string literal")));
        }
    }

    @Override
    protected void doTestDecimalCoerce(String type) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type);

        mapping = mapping.endObject().endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));

        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(GT_FIELD.getPreferredName(), "2.34")
            .field(LT_FIELD.getPreferredName(), "5.67")
            .endObject()
            .endObject()),
            XContentType.JSON));

        ParsedDocument doc2 = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(GT_FIELD.getPreferredName(), "2")
            .field(LT_FIELD.getPreferredName(), "5")
            .endObject()
            .endObject()),
            XContentType.JSON));

        IndexableField[] fields1 = doc1.rootDoc().getFields("field");
        IndexableField[] fields2 = doc2.rootDoc().getFields("field");

        assertEquals(fields1[1].binaryValue(), fields2[1].binaryValue());
    }

    @Override
    protected void doTestNullValue(String type) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type).field("store", true);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        // test null value for min and max
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .nullField(getFromField())
            .nullField(getToField())
            .endObject()
            .endObject()),
            XContentType.JSON));
        assertEquals(3, doc.rootDoc().getFields("field").length);
        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField storedField = fields[2];
        String expected = type.equals("ip_range") ? InetAddresses.toAddrString((InetAddress)getMax(type)) : getMax(type) +"";
        assertThat(storedField.stringValue(), containsString(expected));

        // test null max value
        doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field(getFromField(), getFrom(type))
            .nullField(getToField())
            .endObject()
            .endObject()),
            XContentType.JSON));

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
        doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
            XContentType.JSON));
        assertNull(doc.rootDoc().get("field"));
    }

    public void testNoBounds() throws Exception {
        for (String type : TYPES) {
            doTestNoBounds(type);
        }
    }

    public void doTestNoBounds(String type) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", type).field("store", true);
        if (type.equals("date_range")) {
            mapping = mapping.field("format", DATE_FORMAT);
        }
        mapping = mapping.endObject().endObject().endObject().endObject();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        // test no bounds specified
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .endObject()
            .endObject()),
            XContentType.JSON));

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", RangeType.INTEGER.name)
            .field("format", DATE_FORMAT).endObject().endObject().endObject().endObject();

        ThrowingRunnable runnable = () -> parser.parse("type", new CompressedXContent(Strings.toString(mapping)));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(e.getMessage(), containsString("should not define a dateTimeFormatter"));
    }

    public void testSerializeDefaults() throws Exception {
        for (String type : TYPES) {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject());

            DocumentMapper docMapper = parser.parse("type", new CompressedXContent(mapping));
            RangeFieldMapper mapper = (RangeFieldMapper) docMapper.root().getMapper("field");
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            mapper.doXContentBody(builder, true, ToXContent.EMPTY_PARAMS);
            String got = Strings.toString(builder.endObject());

            // if type is date_range we check that the mapper contains the default format and locale
            // otherwise it should not contain a locale or format
            assertTrue(got, got.contains("\"format\":\"strict_date_optional_time||epoch_millis\"") == type.equals("date_range"));
            assertTrue(got, got.contains("\"locale\":" + "\"" + Locale.ROOT + "\"") == type.equals("date_range"));
        }
    }

    public void testIllegalFormatField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date_range")
                            .array("format", "test_format")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Invalid format: [[test_format]]: Unknown pattern letter: t", e.getMessage());
    }

}
