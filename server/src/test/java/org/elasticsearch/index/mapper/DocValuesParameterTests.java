/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DocValuesParameterTests extends MapperServiceTestCase {

    // -----------------------------------------------------------------------
    // Parsing - accepted forms
    // -----------------------------------------------------------------------

    public void testDocValuesFalseShorthand() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(false));
    }

    public void testMultiValueWithoutCardinalityUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "true").endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(
                new FieldMapper.DocValuesParameter.Values(
                    true,
                    FieldMapper.DocValuesParameter.Values.Cardinality.LOW,
                    FieldMapper.DocValuesParameter.Values.MultiValue.TRUE
                )
            )
        );
    }

    public void testCardinalityWithoutMultiValueUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("cardinality", "high").endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(
                new FieldMapper.DocValuesParameter.Values(
                    true,
                    FieldMapper.DocValuesParameter.Values.Cardinality.HIGH,
                    FieldMapper.DocValuesParameter.Values.MultiValue.TRUE
                )
            )
        );
    }

    public void testBothSubParametersConfigured() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "keyword")
                    .startObject("doc_values")
                    .field("cardinality", "high")
                    .field("multi_value", "preserve_order")
                    .endObject()
            )
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(
                new FieldMapper.DocValuesParameter.Values(
                    true,
                    FieldMapper.DocValuesParameter.Values.Cardinality.HIGH,
                    FieldMapper.DocValuesParameter.Values.MultiValue.PRESERVE_ORDER
                )
            )
        );
    }

    public void testMultiValuePreserveOrderParses() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "preserve_order").endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters().multiValue(),
            equalTo(FieldMapper.DocValuesParameter.Values.MultiValue.PRESERVE_ORDER)
        );
    }

    public void testMultiValueBooleanFalse() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().multiValue(), equalTo(FieldMapper.DocValuesParameter.Values.MultiValue.FALSE));
    }

    // -----------------------------------------------------------------------
    // Parsing - rejected forms
    // -----------------------------------------------------------------------

    public void testInvalidCardinality() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("cardinality", "invalid").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Unknown value [invalid] for field [cardinality] - accepted values are [low, high]"));
    }

    public void testInvalidMultiValue() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "invalid").endObject())
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Unknown value [invalid] for field [multi_value] - accepted values are [false, true, preserve_order]")
        );
    }

    public void testUnknownSubkeyRejected() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("foo", "bar").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Unknown subkey [foo] for [doc_values] on field [field]"));
    }

    public void testCardinalityRejectedOnNumericField() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "long").startObject("doc_values").field("cardinality", "high").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Unknown subkey [cardinality] for [doc_values] on field [field]"));
    }

    // -----------------------------------------------------------------------
    // Serialization (toXContent)
    // -----------------------------------------------------------------------

    public void testToXContentBooleanShorthandPreserved() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        // Text fields default doc_values to disabled, so an explicit `doc_values: true` here is non-default and exercises
        // the boolean-shorthand serialization branch (no sub-parameters configured -> emit boolean).
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "text").field("doc_values", true)));
        Map<String, Object> serialized = serializeFieldMapper(mapperService.documentMapper().mappers().getMapper("field"), false);
        assertThat(serialized.get("doc_values"), is(true));
    }

    public void testToXContentDocValuesFalseEmitsBoolean() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        Map<String, Object> serialized = serializeFieldMapper(mapperService.documentMapper().mappers().getMapper("field"), false);
        assertThat(serialized.get("doc_values"), is(false));
    }

    public void testToXContentEmitsOnlyConfiguredSubparameter() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "false").endObject())
        );
        Map<String, Object> serialized = serializeFieldMapper(mapperService.documentMapper().mappers().getMapper("field"), false);
        assertThat(serialized.get("doc_values"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> dv = (Map<String, Object>) serialized.get("doc_values");
        assertThat(dv.get("multi_value").toString(), equalTo("false"));
        assertThat("cardinality must not be emitted when not configured", dv.get("cardinality"), nullValue());
    }

    public void testToXContentIncludeDefaultsEmitsAllSubparameters() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "false").endObject())
        );
        Map<String, Object> serialized = serializeFieldMapper(mapperService.documentMapper().mappers().getMapper("field"), true);
        assertThat(serialized.get("doc_values"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> dv = (Map<String, Object>) serialized.get("doc_values");
        assertThat(dv.get("multi_value").toString(), equalTo("false"));
        assertThat(dv.get("cardinality").toString(), equalTo("low"));
    }

    // -----------------------------------------------------------------------
    // Builder validation
    // -----------------------------------------------------------------------

    public void testBuilderRequiresEnabled() {
        var e = expectThrows(IllegalStateException.class, () -> FieldMapper.DocValuesParameter.builder(m -> {
            throw new AssertionError("initializer should not be invoked");
        }).build());
        assertThat(e.getMessage(), containsString("enabled() not set"));
    }

    public void testBuilderEnabledCalledTwiceThrows() {
        var b = FieldMapper.DocValuesParameter.builder(m -> { throw new AssertionError("initializer should not be invoked"); })
            .enabled(true);
        var e = expectThrows(IllegalStateException.class, () -> b.enabled(false));
        assertThat(e.getMessage(), containsString("enabled() already set"));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> serializeFieldMapper(Mapper mapper, boolean includeDefaults) throws Exception {
        ToXContent.Params params = includeDefaults ? new ToXContent.MapParams(Map.of("include_defaults", "true")) : ToXContent.EMPTY_PARAMS;
        XContentBuilder builder = jsonBuilder().startObject();
        mapper.toXContent(builder, params).endObject();
        builder.close();
        try (var parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            return (Map<String, Object>) parser.map().get("field");
        }
    }

}
