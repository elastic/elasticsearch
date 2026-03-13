/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MetricTemporalityFieldMapperTests extends MapperTestCase {

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue(
            "metric_temporality field mapper is only available when the feature flag is enabled",
            MetricTemporalityFieldMapper.FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
        b.field("time_series_dimension", true);
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void enableDocValuesOnMapping(XContentBuilder mapping) throws IOException {
        // noop, doc values are always enabled for this field type
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
        return getSampleValueForDocument();
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
                "Failed to parse object: expecting token of type [VALUE_STRING] but found [VALUE_NUMBER]"
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
                String canonical = input.toLowerCase(Locale.ENGLISH);
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
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", MetricTemporalityFieldMapper.CONTENT_TYPE);
            b.field("time_series_dimension", false);
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Field type [metric_temporality] requires [time_series_dimension] to be [true]")
        );
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
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", "gauge")))
        );
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

    public void testRejectsObjectValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startObject("field");
            b.field("foo", "bar");
            b.endObject();
        })));
        assertThat(e.getCause().getMessage(), containsString("Expected a value, but got [START_OBJECT]"));
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
        assumeFalse("metric_temporality does not support multiple values in the same field", true);
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        assumeFalse("This mapper is single-valued and rejects arrays.", true);
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of(new SortShortcutSupport(this::minimalMapping, this::writeField, true));
    }

}
