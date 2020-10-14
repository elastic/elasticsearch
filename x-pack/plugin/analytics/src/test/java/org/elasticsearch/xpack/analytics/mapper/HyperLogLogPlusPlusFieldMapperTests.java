/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperTestCase2;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


public class HyperLogLogPlusPlusFieldMapperTests extends FieldMapperTestCase2<HyperLogLogPlusPlusFieldMapper.Builder> {

    private static final int[] RUNLENS = new int[16];
    static {
        for (int i = 0; i < 16; i++) {
            RUNLENS[i] = i;
        }
    }

    private static final int[] LC_HASHES = new int[] {1, 2};

    private static final long[] MURMUR3_HASHES = new long[] {1, 2};

    private static final int[] RUNLENS1024 = new int[1024];
    static {
        for (int i = 0; i < 1024; i++) {
            RUNLENS1024[i] = i / 128;
        }
    }

    private static final String TYPE = HyperLogLogPlusPlusFieldMapper.CONTENT_TYPE;
    private static final String HLL = HyperLogLogPlusPlusFieldMapper.HLL_FIELD.getPreferredName();
    private static final String LC = HyperLogLogPlusPlusFieldMapper.LC_FIELD.getPreferredName();
    private static final String MURMUR3 = HyperLogLogPlusPlusFieldMapper.MURMUR3_FIELD.getPreferredName();
    private static final String FIELD = "pre_aggregated";

    private DocumentMapper getMapping(int precision, boolean ignoreMalformed) throws IOException {
       return createDocumentMapper(mapping(b -> {
            b.startObject(FIELD).field("type", TYPE);
            if (precision != HyperLogLogPlusPlusFieldMapper.Defaults.PRECISION.value() || randomBoolean()) {
                 b.field("precision", precision);
            }
           if (ignoreMalformed != HyperLogLogPlusPlusFieldMapper.Defaults.IGNORE_MALFORMED.value() || randomBoolean()) {
               b.field("ignore_malformed", ignoreMalformed);
           }
           b.endObject();
        }));
    }

    public void testParseHLLValue() throws Exception {
        DocumentMapper mapper = getMapping(4, randomBoolean());
        ParsedDocument doc = mapper.parse(source(b -> b.startObject(FIELD).field(HLL, RUNLENS).endObject()));
        assertThat(doc.rootDoc().getField(FIELD), notNullValue());
    }

    public void testParseLCValue() throws Exception {
        DocumentMapper mapper = getMapping(4, randomBoolean());
        ParsedDocument doc = mapper.parse(source(b -> b.startObject(FIELD).field(LC, LC_HASHES).endObject()));
        assertThat(doc.rootDoc().getField(FIELD), notNullValue());
    }

    public void testParseMurmur3Value() throws Exception {
        DocumentMapper mapper = getMapping(4, randomBoolean());
        ParsedDocument doc = mapper.parse(source(b -> b.startObject(FIELD).field(MURMUR3, MURMUR3_HASHES).endObject()));
        assertThat(doc.rootDoc().getField(FIELD), notNullValue());
    }

    public void testParseLCAndHLLValue() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b ->
            b.startArray(FIELD)
                .startObject()
                  .field(HLL, RUNLENS)
                  .field(LC, LC_HASHES)
                .endObject()
             .endArray());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expected only one field from [hll], [lc] and [murmur3]"));
    }

    public void testParseArrayValue() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b ->
            b.startArray(FIELD)
              .startObject()
                .field(HLL, RUNLENS)
              .endObject()
              .startObject()
                .field(HLL, RUNLENS)
              .endObject()
            .endArray());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("doesn't not support indexing multiple values " +
            "for the same field in the same document"));
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = getMapping(randomIntBetween(4, 18), randomBoolean());
        ParsedDocument doc = mapper.parse(source(b -> b.nullField(FIELD)));
        assertThat(doc.rootDoc().getField(FIELD), nullValue());
    }

    public void testIgnoreMalformed() throws Exception {
        DocumentMapper mapper = getMapping(randomIntBetween(4, 18), true);
        int[] runLens = new int[17];
        for (int i = 0; i < 17; i++) {
            runLens[i] = i;
        }
        ParsedDocument doc = mapper.parse(source(b -> b.startObject(FIELD).field(HLL, runLens).endObject()));
        assertThat(doc.rootDoc().getField(FIELD), nullValue());
    }

    public void testIgnoreMalformedSkipsKeyword() throws Exception {
        DocumentMapper mapper = getMapping(randomIntBetween(4, 18), true);
        ParsedDocument doc = mapper.parse(source(b -> b.field(FIELD, "value").field("extra", "value")));
        assertThat(doc.rootDoc().getField(FIELD), nullValue());
        assertThat(doc.rootDoc().getField("extra"), notNullValue());
    }

    public void testIgnoreMalformedSkipsArray() throws Exception {
        DocumentMapper mapper = getMapping(randomIntBetween(4, 18), true);
        ParsedDocument doc = mapper.parse(
            source(b -> b.field(FIELD, new int[] {2, 2, 2}).field("extra", "value")));
        assertThat(doc.rootDoc().getField(FIELD), nullValue());
        assertThat(doc.rootDoc().getField("extra"), notNullValue());
    }

    public void testIgnoreMalformedSkipsField() throws Exception {
        DocumentMapper mapper = getMapping(4, true);
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject(FIELD).field("typo", RUNLENS).endObject().field("extra", "value")));
        assertThat(doc.rootDoc().getField(FIELD), nullValue());
        assertThat(doc.rootDoc().getField("extra"), notNullValue());
    }

    public void testIgnoreMalformedSkipsObjects() throws Exception {
        DocumentMapper mapper = getMapping(4, true);
        ParsedDocument doc = mapper.parse(
            source(b ->
                b.startObject(FIELD)
                  .startObject("values")
                    .field("values", new double[] {2, 2})
                    .startObject("object1")
                      .startObject("object2")
                        .field("field", 1)
                      .endObject()
                    .endObject()
                  .endObject()
                  .field("run_lens", RUNLENS)
               .endObject()
               .field("extra", "value")));

        assertThat(doc.rootDoc().getField(FIELD), nullValue());
        assertThat(doc.rootDoc().getField("extra"), notNullValue());
    }

    public void testIgnoreMalformedSkipsEmpty() throws Exception {
        DocumentMapper mapper = getMapping(4, true);
        ParsedDocument doc = mapper.parse(source(b -> b.field(FIELD).startObject().endObject().field("extra", "value")));
        assertThat(doc.rootDoc().getField(FIELD), nullValue());
        assertThat(doc.rootDoc().getField("extra"), notNullValue());
    }

    public void testUnknownField() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b ->
            b.field(FIELD).startObject().field(HLL, RUNLENS).field("unknown", new double[] {2, 2}).endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("with unknown parameter [unknown]"));
    }

    public void testFieldHLLsNotArray() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(HLL, "bah").endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testFieldLCNotArray() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(LC, "bah").endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testFieldMurMur3NotArray() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(MURMUR3, "bah").endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testFieldLCIsZero() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(LC, new int[] {0}).endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[" + LC + "] cannot be 0"));
    }

    public void testFieldLCIsInvalid() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(LC, new int[] {1526283788}).endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[" + LC + "] value is invalid for [1526283788]"));
    }

    public void testRunLenIsLong() throws Exception {
        final long[] runLens = new long[16];
        final int pos = randomIntBetween(0, 15);
        for (int i = 0; i < 16; i++) {
            if (pos == i) {
                runLens[i] =  randomLongBetween((long) Integer.MAX_VALUE + 1, Long.MAX_VALUE);
            } else {
                runLens[i] = i;
            }
        }
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(HLL, runLens).endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString(" out of range of int"));
    }

    public void testFieldNotObject() throws Exception {
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.field(FIELD, "bah"));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_OBJECT] " +
            "but found [VALUE_STRING]"));
    }

    public void testNegativeRunLen() throws Exception {
        final long[] runLens = new long[16];
        final int pos = randomIntBetween(1, 15);
        for (int i = 0; i < 16; i++) {
            if (pos == i) {
                runLens[i] = -i;
            } else {
                runLens[i] = i;
            }
        }
        DocumentMapper mapper = getMapping(4, false);
        SourceToParse source = source(b -> b.startObject(FIELD).field(HLL, runLens).endObject());
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[" + HLL + "] elements must be >= 0 but got " + runLens[pos]));
    }

    public void testMergeField() {
        Mapper.BuilderContext context = new Mapper.BuilderContext(SETTINGS, new ContentPath(1));
        HyperLogLogPlusPlusFieldMapper.Builder builder1 = newBuilder();
        {
            FieldMapper mapper = builder1.build(context);
            HyperLogLogPlusPlusFieldMapper.Builder builder2 = newBuilder();
            builder2.ignoreMalformed(true);
            FieldMapper toMerge = builder2.build(context);
            mapper.merge(toMerge);  // ignore_malformed should merge with no issue
        }
        {
            FieldMapper mapper = builder1.build(context);
            HyperLogLogPlusPlusFieldMapper.Builder builder2 = newBuilder();
            builder2.precision(4);
            FieldMapper toMerge = builder2.build(context);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> mapper.merge(toMerge));
            assertThat(e.getMessage(), containsString("mapper [" + FIELD + "] has different [precision]"));
        }
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values", "store", "index");
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", TYPE);
    }

    @Override
    protected Object getSampleValueForDocument() {
        switch (randomInt(2)) {
            case 0:
                return Map.of(HLL, RUNLENS1024);
            case 1:
                return Map.of(LC, LC_HASHES);
            default:
                return Map.of(MURMUR3, MURMUR3_HASHES);
        }
    }


    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true),
            m -> assertTrue(((HyperLogLogPlusPlusFieldMapper)m).ignoreMalformed()));
    }

    @Override
    protected HyperLogLogPlusPlusFieldMapper.Builder newBuilder() {
        return new HyperLogLogPlusPlusFieldMapper.Builder(FIELD);
    }
}
