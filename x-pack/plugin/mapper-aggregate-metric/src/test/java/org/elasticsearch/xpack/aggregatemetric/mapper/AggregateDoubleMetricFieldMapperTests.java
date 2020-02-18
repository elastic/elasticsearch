/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Names.IGNORE_MALFORMED;
import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Names.METRICS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class AggregateDoubleMetricFieldMapperTests extends ESSingleNodeTestCase {

    public static final String METRICS_FIELD = METRICS.getPreferredName();
    public static final String IGNORE_MALFORMED_FIELD = IGNORE_MALFORMED.getPreferredName();
    public static final String CONTENT_TYPE = AggregateDoubleMetricFieldMapper.CONTENT_TYPE;
    public static final String DEFAULT_METRIC = AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC.getPreferredName();

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testParseValue() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
                .startObject("properties").startObject("metric")
                    .field("type", CONTENT_TYPE)
                    .field(METRICS_FIELD,  new String[] {"min", "max", "sum", "value_count" })
                .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("metric");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                    .field("min", 10.1)
                    .field("max", 50.0)
                    .field("sum", 43)
                    .field("value_count", 14)
                .endObject().endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("metric._min"), notNullValue());
    }

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testInvalidMapping() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("Property [metrics] must be set for field [metric]."));
    }

    /**
     * Test parsing an aggregate_metric field that contains no values
     */
    public void testParseEmptyValue() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max", "sum"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .endObject().endObject()),
                XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("Aggregate metric field [metric] must contain all metrics"));
    }

    /**
     * Test parsing an aggregate_metric field that contains no values
     * when ignore_malformed = true
     */
    public void testParseEmptyValueIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(IGNORE_MALFORMED_FIELD, true)
            .field(METRICS_FIELD,  new String[] {"min", "max", "sum"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .endObject().endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("metric"), nullValue());
    }

    /**
     * Test adding a metric that other than the supported ones (min, max, sum, value_count)
     */
    public void testUnsupportedMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max", "unsupported" })
            .endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> createIndex("test").mapperService().documentMapperParser().parse("_doc", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("Metric [unsupported] is not supported."));
    }

    /**
     * Test inserting a document containing  a metric that has not been defined in the field mapping.
     */
    public void testUnmappedMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("min", 10.0)
                .field("max", 50.0)
                .field("sum", 43)
                .endObject().endObject()),
            XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("Aggregate metric [sum] does not exist in the mapping of field [metric]"));
    }

    /**
     * Test inserting a document containing a metric that has not been defined in the field mapping.
     * Field will be ignored because config ignore_malformed has been set.
     */
    public void testUnmappedMetricWithIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max"})
            .field(IGNORE_MALFORMED_FIELD, true)
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("min", 10.0)
                .field("max", 50.0)
                .field("sum", 43)
                .endObject().endObject()),
            XContentType.JSON));

        assertNull(doc.rootDoc().getField("metric.min"));
    }

    /**
     * Test inserting a document containing less metrics than those defined in the field mapping.
     * An exception will be thrown
     */
    public void testMissingMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max", "sum"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .field("min", 10.0)
                    .field("max", 50.0)
                    .endObject().endObject()),
                XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("Aggregate metric field [metric] must contain all metrics [min, max, sum]"));
    }

    /**
     * Test inserting a document containing less metrics than those defined in the field mapping.
     * Field will be ignored because config ignore_malformed has been set.
     */
    public void testMissingMetricWithIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max", "sum"})
            .field(IGNORE_MALFORMED_FIELD, true)
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("min", 10.0)
                .field("max", 50.0)
                .endObject().endObject()),
            XContentType.JSON));

        assertNull(doc.rootDoc().getField("metric.min"));
    }

    /**
     * Test a metric that has an invalid value (string instead of number)
     */
    public void testInvalidMetricValue() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .field("min", "10.0")
                    .endObject().endObject()),
                XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]"));
    }

    /**
     * Test a metric that has an invalid value (string instead of number)
     * with ignore_malformed = true
     */
    public void testInvalidMetricValueIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min"})
            .field(IGNORE_MALFORMED_FIELD, true)
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .field("min", "10.0")
                    .endObject().endObject()),
                XContentType.JSON));
        assertThat(doc.rootDoc().getField("metric"), nullValue());
    }

    /**
     * Test a field that has a negative value for value_count
     */
    public void testNegativeValueCount() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .field("value_count", -55) // value_count cannot be negative value
                    .endObject().endObject()),
                XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("Aggregate metric [value_count] of field [metric] cannot be a negative number"));
    }

    /**
     * Test a field that has a negative value for value_count with ignore_malformed = true
     * No exception will be thrown but the field will be ignored
     */
    public void testNegativeValueCountIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(IGNORE_MALFORMED_FIELD, true)
            .field(METRICS_FIELD,  new String[] {"value_count"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .field("value_count", -55) // value_count cannot be negative value
                    .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("metric"), nullValue());
    }

    /**
     * Test parsing a value_count metric written as double with zero decimal digits
     */
    public void testValueCountDouble() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count" })
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("metric");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("value_count", 77.0)
                .endObject().endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("metric._value_count"), notNullValue());
    }

    /**
     * Test parsing a value_count metric written as double with some decimal digits
     */
    public void testInvalidDoubleValueCount() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startObject("metric")
                    .field("value_count", 45.43) // Only integers can be a value_count
                    .endObject().endObject()),
                XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("failed to parse field [metric._value_count] of type [integer] in document with id '1'." +
                " Preview of field's value: '45.43'"));
    }

    /**
     * Test inserting a document containing an array of metrics. An exception must be thrown.
     */
    public void testParseArrayValue() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Exception e = expectThrows(MapperParsingException.class,
            () ->defaultMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().startArray("metric")
                    .startObject()
                        .field("min", 10.0)
                        .field("max", 50.0)
                    .endObject().startObject()
                        .field("min", 11.0)
                        .field("max", 51.0)
                    .endObject()
                    .endArray().endObject()),
                XContentType.JSON)));
        assertThat(e.getCause().getMessage(),
            containsString("Field [metric] of type [aggregate_metric_double] does not support " +
                "indexing multiple values for the same field in the same document"));
    }

    /**
     * Test setting the default_metric explicitly
     */
    public void testExplicitDefaultMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count", "sum"})
            .field(DEFAULT_METRIC, "sum")
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("metric");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        assertEquals(AggregateDoubleMetricFieldMapper.Metric.sum, ((AggregateDoubleMetricFieldMapper) fieldMapper).defaultMetric.value());
    }

    /**
     * Test the default_metric when not set explicitly. When only a single metric is contained, this is set as the default
     */
    public void testImplicitDefaultMetricSingleMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("metric");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        assertEquals(AggregateDoubleMetricFieldMapper.Metric.value_count,
            ((AggregateDoubleMetricFieldMapper) fieldMapper).defaultMetric.value());
    }

    /**
     * Test the default_metric when not set explicitly, by default we have set it to be the max.
     */
    public void testImplicitDefaultMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count", "sum", "max"})
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("metric");
        assertThat(fieldMapper, instanceOf(AggregateDoubleMetricFieldMapper.class));
        assertEquals(AggregateDoubleMetricFieldMapper.Metric.max, ((AggregateDoubleMetricFieldMapper) fieldMapper).defaultMetric.value());
    }

    /**
     * Test the default_metric when not set explicitly. When more than one metrics are contained
     * and max is not one of them, an exception should be thrown.
     */
    public void testMissingDefaultMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count", "sum"})
            .endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("Property [default_metric] must be set for field [metric]."));
    }

    /**
     * Test setting an invalid value for the default_metric. An exception must be thrown
     */
    public void testInvalidDefaultMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count", "sum"})
            .field(DEFAULT_METRIC, "invalid_metric")
            .endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> createIndex("test").mapperService().documentMapperParser()
                .parse("_doc", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("Metric [invalid_metric] is not supported."));
    }

    /**
     * Test setting a value for the default_metric that is not contained in the "metrics" field.
     * An exception must be thrown
     */
    public void testUndefinedDefaultMetric() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"value_count", "sum"})
            .field(DEFAULT_METRIC, "min")
            .endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> createIndex("test").mapperService().documentMapperParser()
                .parse("_doc", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("Metric [min] is not defined in the metrics field."));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AggregateMetricMapperPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

}
