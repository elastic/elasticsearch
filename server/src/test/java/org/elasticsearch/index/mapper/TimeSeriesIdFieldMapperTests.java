/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesIdFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return TimeSeriesIdFieldMapper.NAME;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    private DocumentMapper createDocumentMapper(String routingPath, XContentBuilder mappings) throws IOException {
        return createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), 200) // Increase dimension limit
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build(),
            mappings
        ).documentMapper();
    }

    private ParsedDocument parseDocument(DocumentMapper docMapper, CheckedFunction<XContentBuilder, XContentBuilder, IOException> f)
        throws IOException {
        // Add the @timestamp field required by DataStreamTimestampFieldMapper for all time series indices
        return docMapper.parse(source(b -> f.apply(b).field("@timestamp", "2021-10-01")));
    }

    public void testEnabledInTimeSeriesMode() throws Exception {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "long").field("time_series_dimension", true).endObject();
        }));

        ParsedDocument doc = parseDocument(docMapper, b -> b.field("a", "value").field("b", 100).field("c", 500));
        assertThat(
            doc.rootDoc().getBinaryValue("_tsid"),
            equalTo(new BytesRef("\u0002\u0001as\u0005value\u0001bl\u0000\u0000\u0000\u0000\u0000\u0000\u0000d"))
        );
        assertThat(doc.rootDoc().getField("a").binaryValue(), equalTo(new BytesRef("value")));
        assertThat(doc.rootDoc().getField("b").numericValue(), equalTo(100L));
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", "value").entry("b", 100L)
        );
    }

    public void testDisabledInStandardMode() throws Exception {
        DocumentMapper docMapper = createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name()).build(),
            mapping(b -> {})
        ).documentMapper();
        assertThat(docMapper.metadataMapper(TimeSeriesIdFieldMapper.class), is(nullValue()));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));
        assertThat(doc.rootDoc().getBinaryValue("_tsid"), is(nullValue()));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(
            "a",
            mapping(b -> { b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject(); })
        );
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("_tsid", "foo")));

        assertThat(e.getCause().getMessage(), containsString("Field [_tsid] is a metadata field and cannot be added inside a document"));
    }

    /**
     * Test with non-randomized string for sanity checking.
     */
    public void testStrings() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("o")
                .startObject("properties")
                .startObject("e")
                .field("type", "keyword")
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));

        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("a", "foo").field("b", "bar").field("c", "baz").startObject("o").field("e", "bort").endObject()
        );
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", "foo").entry("o.e", "bort")
        );
    }

    public void testKeywordTooLong() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(
            "a",
            mapping(b -> { b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject(); })
        );

        Exception e = expectThrows(
            MapperParsingException.class,
            () -> parseDocument(docMapper, b -> b.field("a", "more_than_1024_bytes".repeat(52)).field("@timestamp", "2021-10-01"))
        );
        assertThat(e.getCause().getMessage(), equalTo("Dimension fields must be less than [1024] bytes but was [1040]."));
    }

    public void testKeywordTooLongUtf8() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(
            "a",
            mapping(b -> { b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject(); })
        );

        String theWordLong = "長い";
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> parseDocument(docMapper, b -> b.field("a", theWordLong.repeat(200)).field("@timestamp", "2021-10-01"))
        );
        assertThat(e.getCause().getMessage(), equalTo("Dimension fields must be less than [1024] bytes but was [1200]."));
    }

    public void testKeywordNull() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(
            "a",
            mapping(b -> { b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject(); })
        );

        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", (String) null)));
        assertThat(e.getCause().getMessage(), equalTo("Dimension fields are missing."));
    }

    /**
     * Test with non-randomized longs for sanity checking.
     */
    public void testLong() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("kw", mapping(b -> {
            b.startObject("kw").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("o")
                .startObject("properties")
                .startObject("e")
                .field("type", "long")
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));

        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("a", 1L).field("b", -1).field("c", "baz").startObject("o").field("e", 1234).endObject()
        );
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", 1L).entry("o.e", 1234L)
        );
    }

    public void testLongInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_a_long")));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [long] in document with id '1'. Preview of field's value: 'not_a_long'")
        );
    }

    public void testLongNull() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", (Long) null)));
        assertThat(e.getCause().getMessage(), equalTo("Dimension fields are missing."));
    }

    /**
     * Test with non-randomized integers for sanity checking.
     */
    public void testInteger() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("kw", mapping(b -> {
            b.startObject("kw").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("o")
                .startObject("properties")
                .startObject("e")
                .field("type", "integer")
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));

        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("a", 1L).field("b", -1).field("c", "baz").startObject("o").field("e", Integer.MIN_VALUE).endObject()
        );
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", 1L).entry("o.e", (long) Integer.MIN_VALUE)
        );
    }

    public void testIntegerInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_an_int")));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [integer] in document with id '1'. Preview of field's value: 'not_an_int'")
        );
    }

    public void testIntegerOutOfRange() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", Long.MAX_VALUE)));
        assertThat(
            e.getMessage(),
            equalTo(
                "failed to parse field [a] of type [integer] in document with id '1'. Preview of field's value: '" + Long.MAX_VALUE + "'"
            )
        );
    }

    /**
     * Test with non-randomized shorts for sanity checking.
     */
    public void testShort() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("kw", mapping(b -> {
            b.startObject("kw").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("o")
                .startObject("properties")
                .startObject("e")
                .field("type", "short")
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));

        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("a", 1L).field("b", -1).field("c", "baz").startObject("o").field("e", Short.MIN_VALUE).endObject()
        );
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", 1L).entry("o.e", (long) Short.MIN_VALUE)
        );
    }

    public void testShortInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_a_short")));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [short] in document with id '1'. Preview of field's value: 'not_a_short'")
        );
    }

    public void testShortOutOfRange() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", Long.MAX_VALUE)));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [short] in document with id '1'. Preview of field's value: '" + Long.MAX_VALUE + "'")
        );
    }

    /**
     * Test with non-randomized shorts for sanity checking.
     */
    public void testByte() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("kw", mapping(b -> {
            b.startObject("kw").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("o")
                .startObject("properties")
                .startObject("e")
                .field("type", "byte")
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));

        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("a", 1L).field("b", -1).field("c", "baz").startObject("o").field("e", (int) Byte.MIN_VALUE).endObject()
        );
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", 1L).entry("o.e", (long) Byte.MIN_VALUE)
        );
    }

    public void testByteInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_a_byte")));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [byte] in document with id '1'. Preview of field's value: 'not_a_byte'")
        );
    }

    public void testByteOutOfRange() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", Long.MAX_VALUE)));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [byte] in document with id '1'. Preview of field's value: '" + Long.MAX_VALUE + "'")
        );
    }

    /**
     * Test with non-randomized ips for sanity checking.
     */
    public void testIp() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("kw", mapping(b -> {
            b.startObject("kw").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "ip").field("time_series_dimension", true).endObject();
            b.startObject("o")
                .startObject("properties")
                .startObject("e")
                .field("type", "ip")
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));

        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.field("a", "192.168.0.1").field("b", -1).field("c", "baz").startObject("o").field("e", "255.255.255.1").endObject()
        );
        assertMap(
            TimeSeriesIdFieldMapper.decodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", "192.168.0.1").entry("o.e", "255.255.255.1")
        );
    }

    public void testIpInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "ip").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_an_ip")));
        assertThat(
            e.getMessage(),
            equalTo("failed to parse field [a] of type [ip] in document with id '1'. Preview of field's value: 'not_an_ip'")
        );
    }

    /**
     * Tests when the total of the tsid is more than 32k.
     */
    public void testVeryLarge() throws IOException {
        // By default, only 16 dimension fields are allowed. To support 100 dimension fields
        // we must increase 'index.mapping.dimension_fields.limit'
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
            for (int i = 0; i < 100; i++) {
                b.startObject("d" + i).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        }));

        String large = "many words ".repeat(80);
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b -> {
            for (int i = 0; i < 100; i++) {
                b.field("d" + i, large);
            }
            return b;
        }));
        assertThat(e.getCause().getMessage(), equalTo("_tsid longer than [32766] bytes [88691]."));
    }

    /**
     * Sending the same document twice produces the same value.
     */
    public void testSameGenConsistentForSameDoc() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "long").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        int b = between(1, 100);
        int c = between(0, 2);
        CheckedFunction<XContentBuilder, XContentBuilder, IOException> fields = d -> d.field("a", a).field("b", b).field("c", (long) c);
        ParsedDocument doc1 = parseDocument(docMapper, fields);
        ParsedDocument doc2 = parseDocument(docMapper, fields);
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, equalTo(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    /**
     * Non-dimension fields don't influence the value of _tsid.
     */
    public void testExtraFieldsDoNotMatter() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "long").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        int b = between(1, 100);
        int c = between(0, 2);
        ParsedDocument doc1 = parseDocument(
            docMapper,
            d -> d.field("a", a).field("b", b).field("c", (long) c).field("e", between(10, 100))
        );
        ParsedDocument doc2 = parseDocument(
            docMapper,
            d -> d.field("a", a).field("b", b).field("c", (long) c).field("e", between(50, 200))
        );
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, equalTo(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    /**
     * The order that the dimensions appear in the document do not influence the value of _tsid.
     */
    public void testOrderDoesNotMatter() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "long").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        int b = between(1, 100);
        int c = between(0, 2);
        ParsedDocument doc1 = parseDocument(docMapper, d -> d.field("a", a).field("b", b).field("c", (long) c));
        ParsedDocument doc2 = parseDocument(docMapper, d -> d.field("b", b).field("a", a).field("c", (long) c));
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, equalTo(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    /**
     * Dimensions that appear in the mapping but not in the document don't influence the value of _tsid.
     */
    public void testUnusedExtraDimensions() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "long").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        int b = between(1, 100);
        CheckedFunction<XContentBuilder, XContentBuilder, IOException> fields = d -> d.field("a", a).field("b", b);
        ParsedDocument doc1 = parseDocument(docMapper, fields);
        ParsedDocument doc2 = parseDocument(docMapper, fields);
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, equalTo(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    /**
     * Different values for dimensions change the result.
     */
    public void testDifferentValues() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        ParsedDocument doc1 = parseDocument(docMapper, d -> d.field("a", a).field("b", between(1, 100)));
        ParsedDocument doc2 = parseDocument(docMapper, d -> d.field("a", a + 1).field("b", between(200, 300)));
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, not(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    /**
     * Two documents with the same *values* but different dimension keys will generate
     * different {@code _tsid}s.
     */
    public void testDifferentDimensions() throws IOException {
        // First doc mapper has dimension fields a and b
        DocumentMapper docMapper1 = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
        }));
        // Second doc mapper has dimension fields a and c
        DocumentMapper docMapper2 = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "integer").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        int b = between(1, 100);
        int c = between(5, 500);
        CheckedFunction<XContentBuilder, XContentBuilder, IOException> fields = d -> d.field("a", a).field("b", b).field("c", c);
        ParsedDocument doc1 = parseDocument(docMapper1, fields);
        ParsedDocument doc2 = parseDocument(docMapper2, fields);
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, not(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    /**
     * Documents with fewer dimensions have a different value.
     */
    public void testFewerDimensions() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "integer").field("time_series_dimension", true).endObject();
        }));

        String a = randomAlphaOfLength(10);
        int b = between(1, 100);
        int c = between(5, 500);
        ParsedDocument doc1 = parseDocument(docMapper, d -> d.field("a", a).field("b", b));
        ParsedDocument doc2 = parseDocument(docMapper, d -> d.field("a", a).field("b", b).field("c", c));
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, not(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    public void testEmpty() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("c").field("type", "integer").field("time_series_dimension", true).endObject();
        }));

        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, d -> d));
        assertThat(e.getCause().getMessage(), equalTo("Dimension fields are missing."));
    }
}
