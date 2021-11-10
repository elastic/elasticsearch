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

import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
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
            TimeSeriesIdFieldMapper.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
            matchesMap().entry("a", "value").entry("b", 100L)
        );
    }

    public void testDisabledInStandardMode() throws Exception {
        DocumentMapper docMapper = createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), "standard").build(),
            mapping(b -> {})
        ).documentMapper();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));

        assertThat(doc.rootDoc().getBinaryValue("_tsid"), is(nullValue()));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> parseDocument(docMapper, b ->  b.field("_tsid", "foo")));

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
            TimeSeriesIdFieldMapper.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
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
            TimeSeriesIdFieldMapper.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
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
            TimeSeriesIdFieldMapper.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
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
            TimeSeriesIdFieldMapper.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
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
            TimeSeriesIdFieldMapper.parse(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)),
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
}
