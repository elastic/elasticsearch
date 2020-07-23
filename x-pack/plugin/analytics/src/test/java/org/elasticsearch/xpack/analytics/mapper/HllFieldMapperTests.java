/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapperTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


public class HllFieldMapperTests extends FieldMapperTestCase<HllFieldMapper.Builder> {

    private static int[] RUNLENS = new int[16];
    static {
        for (int i = 0; i < 16; i++) {
            RUNLENS[i] = i;
        }
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values", "store", "index");
    }

    private static String getMapping(int precision, boolean ignoreMalformed) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
              .startObject("_doc")
                .startObject("properties")
                  .startObject("pre_aggregated")
                    .field("type", "hll")
                    .field("precision", precision)
                    .field("ignore_malformed", ignoreMalformed)
                  .endObject()
               .endObject()
              .endObject()
            .endObject();
        return Strings.toString(xContentBuilder);

    }

    public void testParseValue() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, randomBoolean());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject().field("pre_aggregated")
                           .startObject()
                             .field("sketch", RUNLENS)
                           .endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), notNullValue());
    }

    public void testParseArrayValue() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, false);

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .startArray("pre_aggregated")
                    .startObject()
                      .field("sketch", RUNLENS)
                    .endObject()
                    .startObject()
                      .field("sketch", RUNLENS)
                    .endObject()
                  .endArray()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("doesn't not support indexing multiple values " +
            "for the same field in the same document"));
    }

    public void testNullValue() throws Exception {
        String mapping = getMapping(4, randomBoolean());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().nullField("pre_aggregated")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
    }

    public void testIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = getMapping(4, true);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        int[] runLens = new int[17];
        for (int i = 0; i < 17; i++) {
            runLens[i] = i;
        }

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated").startObject()
                .field("sketch", runLens)
                .endObject()
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
    }

    public void testIgnoreMalformedSkipsKeyword() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, true);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                   .field("pre_aggregated", "value")
                   .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsArray() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, true);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .field("pre_aggregated", new int[] {2, 2, 2})
                  .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsField() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, true);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("pre_aggregated")
                  .startObject()
                    .field("typo", RUNLENS)
                  .endObject()
                  .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsObjects() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, true);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .field("pre_aggregated")
                    .startObject()
                      .startObject("values")
                        .field("values", new double[] {2, 2})
                          .startObject("otherData").
                             startObject("more")
                              .field("toto", 1)
                            .endObject()
                         .endObject()
                     .endObject()
                    .field("run_lens", RUNLENS)
                  .endObject()
                  .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsEmpty() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, true);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .field("pre_aggregated")
                     .startObject()
                     .endObject()
                .field("otherField","value")
                .endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testUnknownField() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, false);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .field("pre_aggregated")
                    .startObject()
                     .field("sketch", RUNLENS)
                     .field("unknown", new double[] {2, 2})
                   .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("with unknown parameter [unknown]"));
    }

    public void testFieldRunLensNotArray() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, false);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .field("pre_aggregated")
                    .startObject()
                      .field("sketch", "bah")
                   .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testRunLenIsLong() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, false);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        long[] runLens = new long[16];
        int pos = randomIntBetween(0, 15);
        for (int i = 0; i < 16; i++) {
            if (pos == i) {
                runLens[i] =  (long) Integer.MAX_VALUE + 1;;
            } else {
                runLens[i] = i;
            }
        }
        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .field("pre_aggregated")
                   .startObject()
                     .field("sketch", runLens)
                  .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString(" out of range of int"));
    }

    public void testFieldNotObject() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, false);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                   .field("pre_aggregated", "bah")
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_OBJECT] " +
            "but found [VALUE_STRING]"));
    }

    public void testNegativeRunLen() throws Exception {
        ensureGreen();
        String mapping = getMapping(4, false);
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        long[] runLens = new long[16];
        int pos = randomIntBetween(0, 15);
        for (int i = 0; i < 16; i++) {
            if (pos == i) {
                runLens[i] = -i;
            } else {
                runLens[i] = i;
            }
        }
        SourceToParse source = new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                  .startObject("pre_aggregated")
                     .field("sketch", runLens)
                  .endObject()
                .endObject()),
            XContentType.JSON);

        Exception e = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[sketch] elements must be >= 0 but got " + runLens[pos]));
    }

    public void testMeta() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
              .startObject("_doc")
                .startObject("properties")
                  .startObject("pre_aggregated")
                    .field("type", "hll")
                    .field("meta", Collections.singletonMap("foo", "bar"))
                    .field("precision", 4)
                  .endObject()
                .endObject()
               .endObject()
            .endObject();

        String mapping = Strings.toString(xContentBuilder);

        IndexService indexService = createIndex("test");
        DocumentMapper mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder()
            .startObject()
              .startObject("_doc")
               .startObject("properties")
                 .startObject("pre_aggregated")
                   .field("type", "hll")
                   .field("precision", 4)
                 .endObject()
                .endObject()
              .endObject()
            .endObject();

        String mapping2 = Strings.toString(xContentBuilder2);
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        XContentBuilder xContentBuilder3 = XContentFactory.jsonBuilder()
            .startObject()
              .startObject("_doc")
                .startObject("properties")
                  .startObject("pre_aggregated")
                    .field("type", "hll")
                    .field("meta", Collections.singletonMap("baz", "quux"))
                    .field("precision", 4)
                  .endObject()
                 .endObject()
              .endObject()
            .endObject();

        String mapping3 = Strings.toString(xContentBuilder3);
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testMergeField() throws IOException {
        String initMapping = getMapping(4, false);
        IndexService indexService = createIndex("test");

        indexService.mapperService().merge("_doc", new CompressedXContent(initMapping),
            MapperService.MergeReason.MAPPING_UPDATE);

        MappedFieldType fieldType = indexService.mapperService().fieldType("pre_aggregated");
        assertThat(fieldType, notNullValue());
        assertThat(fieldType, Matchers.instanceOf(HllFieldMapper.HllFieldType.class));

        assertThat(((HllFieldMapper.HllFieldType) fieldType).precision(), equalTo(4));
        assertThat(((HllFieldMapper.HllFieldType) fieldType).ignoreMalformed(), equalTo(false));

        String updateFormatMapping = getMapping(4, true);

        indexService.mapperService().merge("_doc", new CompressedXContent(updateFormatMapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertThat(indexService.mapperService().fieldType("pre_aggregated"), notNullValue());

        fieldType = indexService.mapperService().fieldType("pre_aggregated");
        assertThat(fieldType, notNullValue());
        assertThat(fieldType, Matchers.instanceOf(HllFieldMapper.HllFieldType.class));

        assertThat(((HllFieldMapper.HllFieldType) fieldType).precision(), equalTo(4));
        assertThat(((HllFieldMapper.HllFieldType) fieldType).ignoreMalformed(), equalTo(true));

        String invalidUpdateFormatMapping = getMapping(5, true);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("_doc", new CompressedXContent(invalidUpdateFormatMapping),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("mapper [pre_aggregated] has different [precision]"));

        fieldType = indexService.mapperService().fieldType("pre_aggregated");
        assertThat(fieldType, notNullValue());
        assertThat(fieldType, Matchers.instanceOf(HllFieldMapper.HllFieldType.class));

        assertThat(((HllFieldMapper.HllFieldType) fieldType).precision(), equalTo(4));
        assertThat(((HllFieldMapper.HllFieldType) fieldType).ignoreMalformed(), equalTo(true));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AnalyticsPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

    @Override
    protected HllFieldMapper.Builder newBuilder() {
        return new HllFieldMapper.Builder("hll");
    }
}
