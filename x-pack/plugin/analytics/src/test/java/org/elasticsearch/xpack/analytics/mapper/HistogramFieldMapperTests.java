/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


public class HistogramFieldMapperTests extends ESSingleNodeTestCase {

    public void testParseValue() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject().field("pre_aggregated").startObject()
                        .field("values", new double[] {2, 3})
                        .field("counts", new int[] {0, 4})
                        .endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), notNullValue());
    }

    public void testParseArrayValue() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("pre_aggregated")
                .startObject()
                .field("counts", new int[] {2, 2, 3})
                .field("values", new double[] {2, 2, 3})
                .endObject()
                .startObject()
                .field("counts", new int[] {2, 2, 3})
                .field("values", new double[] {2, 2, 3})
                .endObject().endArray()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("doesn't not support indexing multiple values " +
            "for the same field in the same document"));
    }

    public void testEmptyArrays() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("values", new double[] {})
                .field("counts", new int[] {})
                .endObject()
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), notNullValue());
    }

    public void testNullValue() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().nullField("pre_aggregated")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
    }

    public void testMissingFieldCounts() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("values", new double[] {2, 2})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expected field called [counts]"));
    }

    public void testIgnoreMalformed() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram")
            .field("ignore_malformed", true);
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("values", new double[] {2, 2})
                .endObject()
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
    }

    public void testIgnoreMalformedSkipsKeyword() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram")
            .field("ignore_malformed", true)
            .endObject().startObject("otherField").field("type", "keyword");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated", "value")
                .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsArray() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram")
            .field("ignore_malformed", true)
            .endObject().startObject("otherField").field("type", "keyword");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated", new int[] {2, 2, 2})
                .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsField() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram")
            .field("ignore_malformed", true)
            .endObject().startObject("otherField").field("type", "keyword");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("values", new double[] {2, 2})
                .field("typo", new double[] {2, 2})
                .endObject()
                .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsObjects() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram")
            .field("ignore_malformed", true)
            .endObject().startObject("otherField").field("type", "keyword");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .startObject("values").field("values", new double[] {2, 2})
                   .startObject("otherData").startObject("more").field("toto", 1)
                   .endObject().endObject()
                .endObject()
                .field("counts", new double[] {2, 2})
                .endObject()
                .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsEmpty() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram")
            .field("ignore_malformed", true)
            .endObject().startObject("otherField").field("type", "keyword");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject().endObject()
                .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testMissingFieldValues() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new int[] {2, 2})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expected field called [values]"));
    }

    public void testUnknownField() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new int[] {2, 2})
                .field("values", new double[] {2, 2})
                .field("unknown", new double[] {2, 2})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("with unknown parameter [unknown]"));
    }

    public void testFieldArraysDifferentSize() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new int[] {2, 2})
                .field("values", new double[] {2, 2, 3})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expected same length from [values] and [counts] but got [3 != 2]"));
    }

    public void testFieldCountsNotArray() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", "bah")
                .field("values", new double[] {2, 2, 3})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testFieldCountsStringArray() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new String[] {"4", "5", "6"})
                .field("values", new double[] {2, 2, 3})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]"));
    }

    public void testFieldValuesStringArray() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new int[] {4, 5, 6})
                .field("values", new String[] {"2", "2", "3"})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]"));
    }

    public void testFieldValuesNotArray() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new int[] {2, 2, 3})
                .field("values", "bah")
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testCountIsLong() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new long[] {2, 2, Long.MAX_VALUE})
                .field("values", new double[] {2 ,2 ,3})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString(" out of range of int"));
    }

    public void testValuesNotInOrder() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("counts", new int[] {2, 8, 4})
                .field("values", new double[] {2 ,3 ,2})
                .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString(" values must be in increasing order, " +
            "got [2.0] but previous value was [3.0]"));
    }

    public void testFieldNotObject() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated", "bah")
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_OBJECT] " +
            "but found [VALUE_STRING]"));
    }

    public void testNegativeCount() throws Exception {
        ensureGreen();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("pre_aggregated").field("type", "histogram");
        String mapping = Strings.toString(xContentBuilder.endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("pre_aggregated")
                .field("counts", new int[] {2, 2, -3})
                .field("values", new double[] {2, 2, 3})
                .endObject().endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[counts] elements must be >= 0 but got -3"));
    }

    public void testMeta() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "histogram")
                .field("meta", Collections.singletonMap("foo", "bar"))
                .endObject().endObject().endObject().endObject());

        IndexService indexService = createIndex("test");
        DocumentMapper mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "histogram")
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "histogram")
                .field("meta", Collections.singletonMap("baz", "quux"))
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AnalyticsPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

}
