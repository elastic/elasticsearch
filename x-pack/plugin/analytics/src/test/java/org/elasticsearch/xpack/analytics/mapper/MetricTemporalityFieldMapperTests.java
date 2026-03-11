/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MetricTemporalityFieldMapperTests extends MapperTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
        b.field("time_series_dimension", true);
    }

    @Override
    public void testMinimalIsInvalidInRoutingPath() throws IOException {
        assumeTrue("metric_temporality field is always a dimension, so it is always valid in the routing path", false);
    }

    @Override
    protected Object getSampleValueForDocument() {
        return randomBoolean() ? "delta" : "cumulative";
    }

    @Override
    protected Object getSampleValueForQuery() {
        return randomBoolean() ? "DELTA" : "CUMULATIVE";
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(
            b -> b.field("ignore_malformed", true),
            m -> assertTrue(((MetricTemporalityFieldMapper) m).ignoreMalformed())
        );
        checker.registerConflictCheck("index", b -> b.field("index", false));
    }


    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue(b -> b.value("unknown")).errorMatches(
                "Unknown value [unknown] for field [metric_temporality] - accepted values are [delta, cumulative]"
            ),
            exampleMalformedValue(b -> b.value(123)).errorMatches(
                "Unknown value [123] for field [metric_temporality] - accepted values are [delta, cumulative]"
            )
        );
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return randomFrom("delta", "cumulative", "DELTA", "CUMULATIVE", null);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) throws IOException {
                String input = randomCaseTemporality();
                String canonical = input.toLowerCase();
                return new SyntheticSourceExample(input, canonical, this::mapping);
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
                b.field("time_series_dimension", true);
                if (ignoreMalformed) {
                    b.field("ignore_malformed", true);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() {
                return List.of();
            }
        };
    }

    private String randomCaseTemporality() {
        String canonical = randomBoolean() ? "delta" : "cumulative";
        if (randomBoolean()) {
            return canonical;
        }
        // upper case a random character to add some variability
        char[] chars = canonical.toCharArray();
        int indexToUpper = randomInt(chars.length - 1);
        chars[indexToUpper] = Character.toUpperCase(chars[indexToUpper]);
        return new String(chars);

    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean dedupAfterFetch() {
        return true;
    }

    public void testDimensionMustBeExplicitlyTrue() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE)))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("Field type [metric_temporality] requires [time_series_dimension] to be [true]")
        );
    }

    public void testDisallowNonDimension() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> {
                b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
                b.field("time_series_dimension", false);
            }))
        );
        assertThat(e.getCause().getMessage(), containsString("Field type [metric_temporality] requires [time_series_dimension] to be [true]"));
    }

    public void testInheritsDimensionFromPassThroughObject() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels")
                .field("type", "passthrough")
                .field("priority", "0")
                .field("time_series_dimension", true)
                .startObject("properties")
                .startObject("temporality")
                .field("type", MetricTemporalityFieldMapper.CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject();
        }));

        Mapper mapper = mapperService.mappingLookup().getMapper("labels.temporality");
        assertTrue(mapper instanceof MetricTemporalityFieldMapper);
        assertTrue(((MetricTemporalityFieldMapper) mapper).fieldType().isDimension());
    }

    public void testCaseInsensitiveAcceptedValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "DeLtA")));
        IndexableField field = doc.rootDoc().getField("field");
        assertThat(field, notNullValue());
        assertThat(field.binaryValue(), equalTo(new BytesRef("delta")));
    }

    public void testIndexedTrue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
            b.field("time_series_dimension", true);
            b.field("index", true);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "delta")));
        IndexableField field = doc.rootDoc().getField("field");
        assertThat(field, notNullValue());
        assertThat(field.fieldType().indexOptions(), equalTo(IndexOptions.DOCS));
    }

    public void testIndexedFalse() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
            b.field("time_series_dimension", true);
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "delta")));
        IndexableField field = doc.rootDoc().getField("field");
        assertThat(field, notNullValue());
        assertThat(field.fieldType().indexOptions(), equalTo(IndexOptions.NONE));
    }

    public void testRejectUnknownValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", "gauge"))));
        assertThat(e.getCause().getMessage(), containsString("accepted values are [delta, cumulative]"));
    }

    public void testIgnoreMalformedSkipsBadValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field")
                .field("type", MetricTemporalityFieldMapper.CONTENT_TYPE)
                .field("time_series_dimension", true)
                .field("ignore_malformed", true)
                .endObject();
            b.startObject("other").field("type", "keyword").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "unknown").field("other", "ok")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getField("other"), notNullValue());
    }

    public void testNullValueAccepted() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    public void testRejectsArrayValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            b.value("delta");
            b.value("cumulative");
            b.endArray();
        })));
        assertThat(e.getCause().getMessage(), containsString("doesn't support indexing multiple values for the same field"));
    }

    public void testRejectsArrayValuesWhenIgnoreMalformedIsTrue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
            b.field("time_series_dimension", true);
            b.field("ignore_malformed", true);
        }));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            b.value("delta");
            b.value("cumulative");
            b.endArray();
        })));
        assertThat(e.getCause().getMessage(), containsString("doesn't support indexing multiple values for the same field"));
    }

    public void testRejectsSubfieldInArrayOfObjects() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field")
                .field("type", "object")
                .startObject("properties")
                .startObject("temporality")
                .field("type", MetricTemporalityFieldMapper.CONTENT_TYPE)
                .field("time_series_dimension", true)
                .endObject()
                .endObject()
                .endObject();
        }));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            b.startObject().field("temporality", "delta").endObject();
            b.startObject().field("temporality", "cumulative").endObject();
            b.endArray();
        })));
        assertThat(e.getCause().getMessage(), containsString("doesn't support indexing multiple values for the same field"));
    }

    public void testRejectsSubfieldInArrayOfObjectsWhenIgnoreMalformedIsTrue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field")
                .field("type", "object")
                .startObject("properties")
                .startObject("temporality")
                .field("type", MetricTemporalityFieldMapper.CONTENT_TYPE)
                .field("time_series_dimension", true)
                .field("ignore_malformed", true)
                .endObject()
                .endObject()
                .endObject();
        }));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            b.startObject().field("temporality", "delta").endObject();
            b.startObject().field("temporality", "cumulative").endObject();
            b.endArray();
        })));
        assertThat(e.getCause().getMessage(), containsString("doesn't support indexing multiple values for the same field"));
    }

    @Override
    protected void assertFetchMany(MapperService mapperService, String field, Object value, String format, int count) throws IOException {
        assumeFalse("metric_temporality does not support multiple values in the same field", false);
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        assumeFalse("This mapper is single-valued and rejects arrays.", false);
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of(new SortShortcutSupport(this::minimalMapping, this::writeField, true));
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }
}
