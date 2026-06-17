/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocValuesParameterTests extends MapperServiceTestCase {

    // -----------------------------------------------------------------------
    // Null-fallback: omitting one sub-parameter falls back to the field-type default
    // -----------------------------------------------------------------------

    public void testMultiValueWithoutCardinalityUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
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
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, true))
        );
    }

    // -----------------------------------------------------------------------
    // Unknown-value rejection
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

    // -----------------------------------------------------------------------
    // Boolean shorthand: doc_values: true / false still works alongside map form
    // -----------------------------------------------------------------------

    public void testDocValuesTrueShorthand() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", true)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(true));
    }

    public void testDocValuesFalseShorthand() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(false));
    }

    // -----------------------------------------------------------------------
    // Index-level setting: index.mapping.doc_values.multi_value
    // -----------------------------------------------------------------------

    /**
     * When the index setting is {@code false} and the field does not set its own {@code doc_values.multi_value}, the field resolves to
     * single-valued.
     */
    public void testIndexSettingFalseDefaultsKeywordToSingleValued() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword")));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
        );
    }

    /**
     * When the index setting is {@code false} and the field does not set its own {@code doc_values.multi_value}, the field resolves to
     * single-valued.
     */
    public void testIndexSettingFalseDefaultsNumberToSingleValued() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "long")));
        NumberFieldMapper mapper = (NumberFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
        );
    }

    /**
     * A field-level {@code doc_values.multi_value: true} overrides the index setting of {@code false}, keeping the field multi-valued.
     */
    public void testFieldLevelTrueOverridesIndexSettingFalse() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", true).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().multiValue(), equalTo(true));
    }

    /**
     * A field-level {@code doc_values.multi_value: false} overrides the index setting of {@code true} (the default), making the field
     * single-valued even though the index-wide default would allow multiple values.
     */
    public void testFieldLevelFalseOverridesIndexSettingTrue() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), true).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().multiValue(), equalTo(false));
    }

    /**
     * Index setting {@code false} causes enforcement: a document carrying two values for a keyword field that did not override the setting
     * is rejected with an {@link IllegalArgumentException} wrapped in {@link DocumentParsingException}.
     */
    public void testIndexSettingFalseEnforcesRejectionOfMultipleValues() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword"))).documentMapper();
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", randomAlphanumericOfLength(4), randomAlphanumericOfLength(4))))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("configured with [multi_value=false] but encountered multiple values in the same document")
        );
    }

    /**
     * With index setting {@code false} but the field overriding with {@code doc_values.multi_value: true}, a document with two values is
     * accepted normally.
     */
    public void testFieldOverrideAllowsMultipleValuesWhenIndexSettingIsFalse() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", true).endObject())
        ).documentMapper();
        // must not throw
        mapper.parse(source(b -> b.array("field", randomAlphanumericOfLength(4), randomAlphanumericOfLength(4))));
    }
}
