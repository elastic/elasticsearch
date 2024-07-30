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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
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
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    @Override
    protected IndexVersion getVersion() {
        return IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.V_8_8_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_ID_HASHING)
        );
    }

    private DocumentMapper createDocumentMapper(String routingPath, XContentBuilder mappings) throws IOException {
        return createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), 200) // Allow tests that use many dimensions
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-10-29T00:00:00Z")
                .build(),
            mappings
        ).documentMapper();
    }

    private static ParsedDocument parseDocument(DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f)
        throws IOException {
        // Add the @timestamp field required by DataStreamTimestampFieldMapper for all time series indices
        return docMapper.parse(source(null, b -> {
            f.accept(b);
            b.field("@timestamp", "2021-10-01");
        }, null));
    }

    private static BytesRef parseAndGetTsid(DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f) throws IOException {
        return parseDocument(docMapper, f).rootDoc().getBinaryValue(TimeSeriesIdFieldMapper.NAME);
    }

    @SuppressWarnings("unchecked")
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
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)), "AWE");
    }

    public void testDisabledInStandardMode() throws Exception {
        DocumentMapper docMapper = createMapperService(
            getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name()).build(),
            mapping(b -> {})
        ).documentMapper();
        assertThat(docMapper.metadataMapper(TimeSeriesIdFieldMapper.class), is(nullValue()));

        ParsedDocument doc = docMapper.parse(source("id", b -> b.field("field", "value"), null));
        assertThat(doc.rootDoc().getBinaryValue("_tsid"), is(nullValue()));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("_tsid", "foo")));

        assertThat(e.getCause().getMessage(), containsString("Field [_tsid] is a metadata field and cannot be added inside a document"));
    }

    /**
     * Test with non-randomized string for sanity checking.
     */
    @SuppressWarnings("unchecked")
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

        BytesRef tsid = parseAndGetTsid(
            docMapper,
            b -> b.field("a", "foo").field("b", "bar").field("c", "baz").startObject("o").field("e", "bort").endObject()
        );
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new BytesArray(tsid).streamInput()), "AWE");
    }

    @SuppressWarnings("unchecked")
    public void testUnicodeKeys() throws IOException {
        String fire = new String(new int[] { 0x1F525 }, 0, 1);
        String coffee = "\u2615";
        DocumentMapper docMapper = createDocumentMapper(fire + "," + coffee, mapping(b -> {
            b.startObject(fire).field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(coffee).field("type", "keyword").field("time_series_dimension", true).endObject();
        }));

        ParsedDocument doc = parseDocument(docMapper, b -> b.field(fire, "hot").field(coffee, "good"));
        Object tsid = TimeSeriesIdFieldMapper.encodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes));
        assertEquals(tsid, "A-I");
    }

    @SuppressWarnings("unchecked")
    public void testKeywordTooLong() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));

        ParsedDocument doc = parseDocument(docMapper, b -> b.field("a", "more_than_1024_bytes".repeat(52)));
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)), "AQ");
    }

    @SuppressWarnings("unchecked")
    public void testKeywordTooLongUtf8() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));

        String theWordLong = "長い";
        ParsedDocument doc = parseDocument(docMapper, b -> b.field("a", theWordLong.repeat(200)));
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)), "AQ");
    }

    public void testKeywordNull() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("r", mapping(b -> {
            b.startObject("r").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));

        BytesRef withNull = parseAndGetTsid(docMapper, b -> b.field("r", "foo").field("a", (String) null));
        BytesRef withoutField = parseAndGetTsid(docMapper, b -> b.field("r", "foo"));
        assertThat(withNull, equalTo(withoutField));
    }

    /**
     * Test with non-randomized longs for sanity checking.
     */
    @SuppressWarnings("unchecked")
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

        BytesRef tsid = parseAndGetTsid(docMapper, b -> {
            b.field("kw", "kw");
            b.field("a", 1L);
            b.field("b", -1);
            b.field("c", "baz");
            b.startObject("o").field("e", 1234).endObject();
        });
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new BytesArray(tsid).streamInput()), "AWFs");
    }

    public void testLongInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_a_long")));
        assertThat(
            e.getMessage(),
            // TODO describe the document instead of "null"
            equalTo("[1:6] failed to parse field [a] of type [long] in a time series document. Preview of field's value: 'not_a_long'")
        );
    }

    public void testLongNull() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("r", mapping(b -> {
            b.startObject("r").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("a").field("type", "long").field("time_series_dimension", true).endObject();
        }));

        BytesRef withNull = parseAndGetTsid(docMapper, b -> b.field("r", "foo").field("a", (Long) null));
        BytesRef withoutField = parseAndGetTsid(docMapper, b -> b.field("r", "foo"));
        assertThat(withNull, equalTo(withoutField));
    }

    /**
     * Test with non-randomized integers for sanity checking.
     */
    @SuppressWarnings("unchecked")
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

        BytesRef tsid = parseAndGetTsid(docMapper, b -> {
            b.field("kw", "kw");
            b.field("a", 1L);
            b.field("b", -1);
            b.field("c", "baz");
            b.startObject("o").field("e", Integer.MIN_VALUE).endObject();
        });
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new BytesArray(tsid).streamInput()), "AWFs");
    }

    public void testIntegerInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_an_int")));
        assertThat(
            e.getMessage(),
            equalTo("[1:6] failed to parse field [a] of type [integer] in a time series document. Preview of field's value: 'not_an_int'")
        );
    }

    public void testIntegerOutOfRange() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", Long.MAX_VALUE)));
        assertThat(
            e.getMessage(),
            equalTo(
                "[1:6] failed to parse field [a] of type [integer] in a time series document. Preview of field's value: '"
                    + Long.MAX_VALUE
                    + "'"
            )
        );
    }

    /**
     * Test with non-randomized shorts for sanity checking.
     */
    @SuppressWarnings("unchecked")
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

        BytesRef tsid = parseAndGetTsid(docMapper, b -> {
            b.field("kw", "kw");
            b.field("a", 1L);
            b.field("b", -1);
            b.field("c", "baz");
            b.startObject("o").field("e", Short.MIN_VALUE).endObject();
        });
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new BytesArray(tsid).streamInput()), "AWFs");
    }

    public void testShortInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_a_short")));
        assertThat(
            e.getMessage(),
            equalTo("[1:6] failed to parse field [a] of type [short] in a time series document. Preview of field's value: 'not_a_short'")
        );
    }

    public void testShortOutOfRange() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", Long.MAX_VALUE)));
        assertThat(
            e.getMessage(),
            equalTo(
                "[1:6] failed to parse field [a] of type [short] in a time series document. Preview of field's value: '"
                    + Long.MAX_VALUE
                    + "'"
            )
        );
    }

    /**
     * Test with non-randomized shorts for sanity checking.
     */
    @SuppressWarnings("unchecked")
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

        BytesRef tsid = parseAndGetTsid(docMapper, b -> {
            b.field("kw", "kw");
            b.field("a", 1L);
            b.field("b", -1);
            b.field("c", "baz");
            b.startObject("o").field("e", (int) Byte.MIN_VALUE).endObject();
        });
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new BytesArray(tsid).streamInput()), "AWFs");
    }

    public void testByteInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_a_byte")));
        assertThat(
            e.getMessage(),
            equalTo("[1:6] failed to parse field [a] of type [byte] in a time series document. Preview of field's value: 'not_a_byte'")
        );
    }

    public void testByteOutOfRange() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", Long.MAX_VALUE)));
        assertThat(
            e.getMessage(),
            equalTo(
                "[1:6] failed to parse field [a] of type [byte] in a time series document. Preview of field's value: '"
                    + Long.MAX_VALUE
                    + "'"
            )
        );
    }

    /**
     * Test with non-randomized ips for sanity checking.
     */
    @SuppressWarnings("unchecked")
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

        ParsedDocument doc = parseDocument(docMapper, b -> {
            b.field("kw", "kw");
            b.field("a", "192.168.0.1");
            b.field("b", -1);
            b.field("c", "baz");
            b.startObject("o").field("e", "255.255.255.1").endObject();
        });
        assertEquals(TimeSeriesIdFieldMapper.encodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes)), "AWFz");
    }

    public void testIpInvalidString() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("a").field("type", "ip").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> parseDocument(docMapper, b -> b.field("a", "not_an_ip")));
        assertThat(
            e.getMessage(),
            equalTo("[1:6] failed to parse field [a] of type [ip] in a time series document. Preview of field's value: 'not_an_ip'")
        );
    }

    /**
     * Tests when the total of the tsid is more than 32k.
     */
    @SuppressWarnings("unchecked")
    public void testVeryLarge() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("b", mapping(b -> {
            b.startObject("b").field("type", "keyword").field("time_series_dimension", true).endObject();
            for (int i = 0; i < 100; i++) {
                b.startObject("d" + i).field("type", "keyword").field("time_series_dimension", true).endObject();
            }
        }));

        String large = "many words ".repeat(80);
        ParsedDocument doc = parseDocument(docMapper, b -> {
            b.field("b", "foo");
            for (int i = 0; i < 100; i++) {
                b.field("d" + i, large);
            }
        });

        Object tsid = TimeSeriesIdFieldMapper.encodeTsid(new ByteArrayStreamInput(doc.rootDoc().getBinaryValue("_tsid").bytes));
        assertEquals(
            tsid,
            "AWJzA2ZvbwJkMHPwBm1hbnkgd29yZHMgbWFueSB3b3JkcyBtYW55IHdvcmRzIG1hbnkgd29yZHMgbWFueSB3b3JkcyBtYW55IHdvcmRzIG1hbnkgd"
                + "29yZHMgbWFueSB3b3JkcyA"
        );
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
        CheckedConsumer<XContentBuilder, IOException> fields = d -> d.field("a", a).field("b", b).field("c", (long) c);
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
        CheckedConsumer<XContentBuilder, IOException> fields = d -> d.field("a", a).field("b", b);
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

    public void testSameMetricNamesDifferentValues() throws IOException {
        DocumentMapper docMapper = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("m1").field("type", "double").field("time_series_metric", "gauge").endObject();
            b.startObject("m2").field("type", "integer").field("time_series_metric", "counter").endObject();
        }));

        ParsedDocument doc1 = parseDocument(
            docMapper,
            d -> d.field("a", "value")
                .field("b", 10)
                .field("m1", randomDoubleBetween(100, 200, true))
                .field("m2", randomIntBetween(100, 200))
        );
        ParsedDocument doc2 = parseDocument(
            docMapper,
            d -> d.field("a", "value").field("b", 10).field("m1", randomDoubleBetween(10, 20, true)).field("m2", randomIntBetween(10, 20))
        );
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, equalTo(doc2.rootDoc().getBinaryValue("_tsid").bytes));
    }

    public void testDifferentMetricNamesSameValues() throws IOException {
        DocumentMapper docMapper1 = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("m1").field("type", "double").field("time_series_metric", "gauge").endObject();
        }));

        DocumentMapper docMapper2 = createDocumentMapper("a", mapping(b -> {
            b.startObject("a").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("b").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("m2").field("type", "double").field("time_series_metric", "gauge").endObject();
        }));

        double metricValue = randomDoubleBetween(10, 20, true);
        ParsedDocument doc1 = parseDocument(docMapper1, d -> d.field("a", "value").field("b", 10).field("m1", metricValue));
        ParsedDocument doc2 = parseDocument(docMapper2, d -> d.field("a", "value").field("b", 10).field("m2", metricValue));
        // NOTE: plain tsid (not hashed) does not take metric names/values into account
        assertThat(doc1.rootDoc().getBinaryValue("_tsid").bytes, equalTo(doc2.rootDoc().getBinaryValue("_tsid").bytes));
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
        CheckedConsumer<XContentBuilder, IOException> fields = d -> d.field("a", a).field("b", b).field("c", c);
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

    public void testParseWithDynamicMapping() {
        Settings indexSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .build();
        MapperService mapper = createMapperService(IndexVersion.current(), indexSettings, () -> false);
        SourceToParse source = new SourceToParse(null, new BytesArray("""
            {
                "@timestamp": 1609459200000,
                "dim": "6a841a21",
                "value": 100
            }"""), XContentType.JSON, TimeSeriesRoutingHashFieldMapper.DUMMY_ENCODED_VALUE);
        Engine.Index index = IndexShard.prepareIndex(
            mapper,
            source,
            UNASSIGNED_SEQ_NO,
            randomNonNegativeLong(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0,
            System.nanoTime()
        );
        assertNotNull(index.parsedDoc().dynamicMappingsUpdate());
    }

    public void testParseWithDynamicMappingInvalidRoutingHash() {
        Settings indexSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .build();
        MapperService mapper = createMapperService(IndexVersion.current(), indexSettings, () -> false);
        SourceToParse source = new SourceToParse(null, new BytesArray("""
            {
                "@timestamp": 1609459200000,
                "dim": "6a841a21",
                "value": 100
            }"""), XContentType.JSON, "no such routing hash");
        var failure = expectThrows(DocumentParsingException.class, () -> {
            IndexShard.prepareIndex(
                mapper,
                source,
                UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                -1,
                false,
                UNASSIGNED_SEQ_NO,
                0,
                System.nanoTime()
            );
        });
        assertThat(failure.getMessage(), equalTo("[5:1] failed to parse: Illegal base64 character 20"));
    }
}
