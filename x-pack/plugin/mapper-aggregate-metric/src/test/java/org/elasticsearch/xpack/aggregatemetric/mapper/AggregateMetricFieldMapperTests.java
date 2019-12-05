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

import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricFieldMapper.Names.IGNORE_MALFORMED;
import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricFieldMapper.Names.METRICS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class AggregateMetricFieldMapperTests extends ESSingleNodeTestCase {

    public static final String METRICS_FIELD = METRICS.getPreferredName();
    public static final String CONTENT_TYPE = AggregateMetricFieldMapper.CONTENT_TYPE;

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testParseValue() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
                .startObject("properties").startObject("metric")
                    .field("type", CONTENT_TYPE)
                    .field(METRICS_FIELD,  new String[] {"min", "max", "sum" })
                .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("_doc", new CompressedXContent(mapping));

        Mapper fieldMapper = defaultMapper.mappers().getMapper("metric");
        assertThat(fieldMapper, instanceOf(AggregateMetricFieldMapper.class));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                    .field("min", 10.0)
                    .field("max", 50.0)
                    .field("sum", 43)
                .endObject().endObject()),
            XContentType.JSON));

        assertThat(doc.rootDoc().getField("metric"), notNullValue());
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
            .field(IGNORE_MALFORMED, true)
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

        assertThat(doc.rootDoc().getField("metric"), notNullValue());
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

        Exception e = expectThrows(MapperParsingException.class,
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
     * Metric will be ignored because config ignore_malformed has been set.
     */
    public void testUnmappedMetricWithIgnoreMalformed() throws Exception {
        ensureGreen();

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max"})
            .field(IGNORE_MALFORMED, true)
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

        assertThat(doc.rootDoc().getField("metric"), notNullValue());
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
            containsString("Aggregate metric [min] of field [metric] must be a number"));
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
            containsString("Aggregate metric [value_count] of field [metric] must not be a negative number"));
    }

    /**
     * Test a field that has a negative value for value_count
     */
    public void testInvalidValueCount() throws Exception {
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
            containsString("Aggregate metric [value_count] of field [metric] must be an integer number"));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AggregateMetricMapperPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

}
