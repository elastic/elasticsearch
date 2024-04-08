/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Names.METRICS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class AggregateDoubleMetricFieldMapperTests extends MapperTestCase {

    public static final String METRICS_FIELD = METRICS;
    public static final String CONTENT_TYPE = AggregateDoubleMetricFieldMapper.CONTENT_TYPE;
    public static final String DEFAULT_METRIC = AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC;

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AggregateMetricMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "min", "max", "value_count" }).field(DEFAULT_METRIC, "max");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(DEFAULT_METRIC, fieldMapping(this::minimalMapping), fieldMapping(b -> {
            b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "min", "max" }).field(DEFAULT_METRIC, "min");
        }));

        checker.registerConflictCheck(METRICS_FIELD, fieldMapping(this::minimalMapping), fieldMapping(b -> {
            b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "min", "max" }).field(DEFAULT_METRIC, "max");
        }));

        checker.registerConflictCheck(METRICS_FIELD, fieldMapping(this::minimalMapping), fieldMapping(b -> {
            b.field("type", CONTENT_TYPE)
                .field(METRICS_FIELD, new String[] { "min", "max", "value_count", "sum" })
                .field(DEFAULT_METRIC, "min");
        }));
    }

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("min", -10.1, "max", 50.0, "value_count", 14);
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected Object getSampleValueForQuery() {
        return 50.0;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testParseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field("min", -10.1).field("max", 50.0).field("value_count", 14).endObject())
        );
        assertEquals("DoubleField <field.min:-10.1>", doc.rootDoc().getField("field.min").toString());

        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
    }

    /**
     * Test that invalid field mapping containing no metrics is not accepted
     */
    public void testInvalidMapping() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("metric")
            .field("type", CONTENT_TYPE)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping));
        assertThat(e.getMessage(), containsString("Property [metrics] is required for field [metric]."));
    }

    /**
     * Test parsing an aggregate_metric field that contains no values
     */
    public void testParseEmptyValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.startObject("field").endObject())));
        assertThat(
            e.getCause().getMessage(),
            containsString("Aggregate metric field [field] must contain all metrics [min, max, value_count]")
        );
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            // no metrics
            exampleMalformedValue(b -> b.startObject().endObject()).errorMatches(
                "Aggregate metric field [field] must contain all metrics [min, max, value_count]"
            ),
            // unmapped metric
            exampleMalformedValue(
                b -> b.startObject().field("min", -10.1).field("max", 50.0).field("value_count", 14).field("sum", 55).endObject()
            ).errorMatches("Aggregate metric [sum] does not exist in the mapping of field [field]"),
            // missing metric
            exampleMalformedValue(b -> b.startObject().field("min", -10.1).field("max", 50.0).endObject()).errorMatches(
                "Aggregate metric field [field] must contain all metrics [min, max, value_count]"
            ),
            // invalid metric value
            exampleMalformedValue(b -> b.startObject().field("min", "10.0").field("max", 50.0).field("value_count", 14).endObject())
                .errorMatches("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]"),
            // negative value count
            exampleMalformedValue(b -> b.startObject().field("min", 10.0).field("max", 50.0).field("value_count", -14).endObject())
                .errorMatches("Aggregate metric [value_count] of field [field] cannot be a negative number"),
            // value count with decimal digits (whole numbers formatted as doubles are permitted, but non-whole numbers are not)
            exampleMalformedValue(b -> b.startObject().field("min", 10.0).field("max", 50.0).field("value_count", 77.33).endObject())
                .errorMatches("failed to parse [value_count] sub field: 77.33 cannot be converted to Integer without data loss")
        );
    }

    /**
     * Test adding a metric that other than the supported ones (min, max, sum, value_count)
     */
    public void testUnsupportedMetric() throws Exception {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(b -> b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "min", "max", "unsupported" }))
            )
        );
        assertThat(e.getMessage(), containsString("Metric [unsupported] is not supported."));
    }

    /**
     * Test parsing a value_count metric written as double with zero decimal digits
     */
    public void testValueCountDouble() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field("min", 10.0).field("max", 50.0).field("value_count", 77.0).endObject())
        );
        assertEquals(77, doc.rootDoc().getField("field.value_count").numericValue().longValue());
    }

    /**
     * Test parsing a metric and check the min max value
     */
    public void testCheckMinMaxValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // min > max
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(
                source(b -> b.startObject("field").field("min", 50.0).field("max", 10.0).field("value_count", 14).endObject())
            )
        );
        assertThat(e.getCause().getMessage(), containsString("Aggregate metric field [field] max value cannot be smaller than min value"));

        // min == max
        mapper.parse(source(b -> b.startObject("field").field("min", 50.0).field("max", 50.0).field("value_count", 14).endObject()));

        // min < max
        mapper.parse(source(b -> b.startObject("field").field("min", 10.0).field("max", 50.0).field("value_count", 14).endObject()));
    }

    private void randomMapping(XContentBuilder b, int randomNumber) throws IOException {
        b.field("type", CONTENT_TYPE);
        switch (randomNumber) {
            case 0 -> b.field(METRICS_FIELD, new String[] { "min" }).field(DEFAULT_METRIC, "min");
            case 1 -> b.field(METRICS_FIELD, new String[] { "max" }).field(DEFAULT_METRIC, "max");
            case 2 -> b.field(METRICS_FIELD, new String[] { "value_count" }).field(DEFAULT_METRIC, "value_count");
            case 3 -> b.field(METRICS_FIELD, new String[] { "sum" }).field(DEFAULT_METRIC, "sum");
        }
    }

    /**
     * Test inserting a document containing an array of metrics. An exception must be thrown.
     */
    public void testParseArrayValue() throws Exception {
        int randomNumber = randomIntBetween(0, 3);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> randomMapping(b, randomNumber)));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field").startObject();
            switch (randomNumber) {
                case 0 -> b.field("min", 10.0);
                case 1 -> b.field("max", 50);
                case 2 -> b.field("value_count", 3);
                case 3 -> b.field("sum", 100.0);
            }
            b.endObject();
            b.startObject();
            switch (randomNumber) {
                case 0 -> b.field("min", 20.0);
                case 1 -> b.field("max", 60);
                case 2 -> b.field("value_count", 2);
                case 3 -> b.field("sum", 200.0);
            }

            b.endObject().endArray();
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "Field [field] of type [aggregate_metric_double] "
                    + "does not support indexing multiple values for the same field in the same document"
            )
        );
    }

    /**
     * Test setting the default_metric explicitly
     */
    public void testExplicitDefaultMetric() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "value_count", "sum" }).field(DEFAULT_METRIC, "sum")
            )
        );

        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        assertEquals(Metric.sum, ((AggregateDoubleMetricFieldMapper) fieldMapper).defaultMetric());
    }

    /**
     * Test the default_metric when not set explicitly. When only a single metric is contained, this is set as the default
     */
    public void testImplicitDefaultMetricSingleMetric() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "value_count" }))
        );

        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        assertEquals(Metric.value_count, ((AggregateDoubleMetricFieldMapper) fieldMapper).defaultMetric);
    }

    /**
     * Test the default_metric when not set explicitly, by default we have set it to be the max.
     */
    public void testImplicitDefaultMetric() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        assertEquals(Metric.max, ((AggregateDoubleMetricFieldMapper) fieldMapper).defaultMetric);
    }

    /**
     * Test the default_metric when not set explicitly. When more than one metrics are contained
     * and max is not one of them, an exception should be thrown.
     */
    public void testMissingDefaultMetric() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(b -> b.field("type", CONTENT_TYPE).field(METRICS_FIELD, new String[] { "value_count", "sum" }))
            )
        );
        assertThat(e.getMessage(), containsString("Property [default_metric] is required for field [field]."));
    }

    /**
     * Test setting an invalid value for the default_metric. An exception must be thrown
     */
    public void testInvalidDefaultMetric() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", CONTENT_TYPE)
                        .field(METRICS_FIELD, new String[] { "value_count", "sum" })
                        .field(DEFAULT_METRIC, "invalid_metric")
                )
            )
        );
        assertThat(e.getMessage(), containsString("Metric [invalid_metric] is not supported."));
    }

    /**
     * Test setting a value for the default_metric that is not contained in the "metrics" field.
     * An exception must be thrown
     */
    public void testUndefinedDefaultMetric() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", CONTENT_TYPE)
                        .field(METRICS_FIELD, new String[] { "value_count", "sum" })
                        .field(DEFAULT_METRIC, "min")
                )
            )
        );
        assertThat(e.getMessage(), containsString("Default metric [min] is not defined in the metrics of field [field]."));
    }

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testParseNestedValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.startObject("properties")
                    .startObject("subfield")
                    .field("type", CONTENT_TYPE)
                    .field(METRICS_FIELD, new String[] { "min", "max", "sum", "value_count" })
                    .field(DEFAULT_METRIC, "max")
                    .endObject()
                    .endObject()
            )
        );

        Mapper fieldMapper = mapper.mappers().getMapper("field.subfield");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        ParsedDocument doc = mapper.parse(
            source(
                b -> b.startObject("field")
                    .startObject("subfield")
                    .field("min", 10.1)
                    .field("max", 50.0)
                    .field("sum", 43)
                    .field("value_count", 14)
                    .endObject()
                    .endObject()
            )
        );
        assertThat(doc.rootDoc().getField("field.subfield.min"), notNullValue());
    }

    /**
     *  subfields of aggregate_metric_double should not be searchable or exposed in field_caps
     */
    public void testNoSubFieldsIterated() throws IOException {
        Metric[] values = Metric.values();
        List<Metric> subset = randomSubsetOf(randomIntBetween(1, values.length), values);
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", CONTENT_TYPE).field(METRICS_FIELD, subset).field(DEFAULT_METRIC, subset.get(0)))
        );
        Iterator<Mapper> iterator = mapper.mappers().getMapper("field").iterator();
        assertFalse(iterator.hasNext());
    }

    public void testFieldCaps() throws IOException {
        MapperService aggMetricMapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = aggMetricMapperService.fieldType("field");
        assertThat(fieldType.familyTypeName(), equalTo("aggregate_metric_double"));
        assertTrue(fieldType.isSearchable());
        assertTrue(fieldType.isAggregatable());
    }

    /*
     * Since all queries for aggregate_metric_double fields are delegated to their default_metric numeric
     *  sub-field, we override this method so that testExistsQueryMinimalMapping() passes successfully.
     */
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, Matchers.instanceOf(FieldExistsQuery.class));
        FieldExistsQuery fieldExistsQuery = (FieldExistsQuery) query;
        String defaultMetric = ((AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType) fieldType).getDefaultMetric().name();
        assertEquals("field." + defaultMetric, fieldExistsQuery.getField());
        assertNoFieldNamesField(fields);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    public void testCannotBeUsedInMultifields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("metric");
            minimalMapping(b);
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [metric] of type [aggregate_metric_double] can't be used in multifields"));
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType ft =
            (AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());
        assertMetricType("gauge", AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType::getMetricType);

        {
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "counter");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [counter] for field [time_series_metric] - accepted values are [gauge]")
            );
        }
        {
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "unknown");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [unknown] for field [time_series_metric] - accepted values are [gauge]")
            );
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assumeFalse("synthetic _source support for aggregate_double_metric doesn't support ignore_malformed", ignoreMalformed);
        return new AggregateDoubleMetricSyntheticSourceSupport();
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    protected final class AggregateDoubleMetricSyntheticSourceSupport implements SyntheticSourceSupport {

        private final EnumSet<Metric> storedMetrics = EnumSet.copyOf(randomNonEmptySubsetOf(Arrays.asList(Metric.values())));

        @Override
        public SyntheticSourceExample example(int maxVals) {
            // aggregate_metric_double field does not support arrays
            Map<String, Object> value = randomAggregateMetric();
            return new SyntheticSourceExample(value, value, this::mapping);
        }

        private Map<String, Object> randomAggregateMetric() {
            Map<String, Object> value = new LinkedHashMap<>(storedMetrics.size());
            for (Metric m : storedMetrics) {
                if (Metric.value_count == m) {
                    value.put(m.name(), randomLongBetween(1, 1_000_000));
                } else if (Metric.max == m) {
                    value.put(m.name(), randomDoubleBetween(100d, 1_000_000d, false));
                } else if (Metric.min == m) {
                    value.put(m.name(), randomDoubleBetween(-1_000_000d, 99d, false));
                } else {
                    value.put(m.name(), randomDouble());
                }
            }
            return value;
        }

        private void mapping(XContentBuilder b) throws IOException {
            String[] metrics = storedMetrics.stream().map(Metric::toString).toArray(String[]::new);
            b.field("type", CONTENT_TYPE).array(METRICS_FIELD, metrics).field(DEFAULT_METRIC, metrics[0]);
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of(
                new SyntheticSourceInvalidExample(
                    matchesPattern("field \\[field] of type \\[.+] doesn't support synthetic source because it ignores malformed numbers"),
                    b -> {
                        mapping(b);
                        b.field("ignore_malformed", true);
                    }
                )
            );
        }
    }

    @Override
    protected boolean supportsCopyTo() {
        return false;
    }
}
