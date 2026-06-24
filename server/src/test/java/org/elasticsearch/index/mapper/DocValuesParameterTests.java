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
import org.elasticsearch.index.IndexMode;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocValuesParameterTests extends MapperServiceTestCase {

    // -----------------------------------------------------------------------
    // Null-fallback: omitting one sub-parameter falls back to the field-type default
    // -----------------------------------------------------------------------

    public void testMultiValueWithoutCardinalityUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false, true))
        );
    }

    public void testCardinalityWithoutMultiValueUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("cardinality", "high").endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, true, true))
        );
    }

    public void testNullabilityWithoutOtherSubParametersUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, true, false))
        );
    }

    // -----------------------------------------------------------------------
    // Unknown-value rejection
    // -----------------------------------------------------------------------

    public void testInvalidCardinality() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
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
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", true)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(true));
    }

    public void testDocValuesFalseShorthand() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
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
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword")));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false, true))
        );
    }

    /**
     * When the index setting is {@code false} and the field does not set its own {@code doc_values.multi_value}, the field resolves to
     * single-valued.
     */
    public void testIndexSettingFalseDefaultsNumberToSingleValued() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "long")));
        NumberFieldMapper mapper = (NumberFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false, true))
        );
    }

    /**
     * A field-level {@code doc_values.multi_value: true} overrides the index setting of {@code false}, keeping the field multi-valued.
     */
    public void testFieldLevelTrueOverridesIndexSettingFalse() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
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
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
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
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
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
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", true).endObject())
        ).documentMapper();
        // must not throw
        mapper.parse(source(b -> b.array("field", randomAlphanumericOfLength(4), randomAlphanumericOfLength(4))));
    }

    // -----------------------------------------------------------------------
    // Index-level setting: index.mapping.doc_values.nullabilty
    // -----------------------------------------------------------------------

    /**
     * When the index setting is {@code false} and the field does not set its own {@code doc_values.nullability}, the keyword field
     * resolves to non-nullable.
     */
    public void testIndexSettingFalseDefaultsKeywordToNonNullable() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword")));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, true, false))
        );
    }

    /**
     * When the index setting is {@code false} and the field does not set its own {@code doc_values.nullability}, the number field
     * resolves to non-nullable.
     */
    public void testIndexSettingFalseDefaultsNumberToNonNullable() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "long")));
        NumberFieldMapper mapper = (NumberFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, true, false))
        );
    }

    /**
     * A field-level {@code doc_values.nullability: true} overrides the index setting of {@code false}, keeping the field nullable.
     */
    public void testFieldLevelTrueOverridesIndexSettingFalseForNullability() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", true).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().nullability(), equalTo(true));
    }

    /**
     * A field-level {@code doc_values.nullability: false} overrides the index default of {@code true}, making the field non-nullable
     * even though the index-wide default would allow nulls.
     */
    public void testFieldLevelFalseOverridesIndexSettingTrueForNullability() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), true).build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> {
            b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject();
        }));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().nullability(), equalTo(false));
    }

    /**
     * Index setting {@code false} causes enforcement: a document carrying a null value for a keyword field that did not override the
     * setting is rejected with an {@link IllegalArgumentException} wrapped in {@link DocumentParsingException}.
     */
    public void testIndexSettingFalseEnforcesRejectionOfNullValue() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword"))).documentMapper();
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", (Object) null)))
        );
        assertThat(e.getCause().getMessage(), containsString("configured with [nullability=false] but were null"));
    }

    public void testIndexSettingFalseMultiValueContainsNullDoesntReject() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword"))).documentMapper();
        mapper.parse(source(b -> b.array("field", "asdf", (Object) null)));
    }

    /**
     * With index setting {@code false} but the field overriding with {@code doc_values.nullability: true}, a document with null values is
     * accepted normally.
     */
    public void testFieldOverrideAllowsNullValueWhenIndexSettingIsFalse() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", true).endObject())
        ).documentMapper();
        // must not throw
        mapper.parse(source(b -> b.array("field", (Object) null)));
    }

    /**
     * Index setting {@code false} causes enforcement for number fields: a document carrying a null value for a long field that did not
     * override the setting is rejected with an {@link IllegalArgumentException} wrapped in {@link DocumentParsingException}.
     */
    public void testIndexSettingFalseEnforcesRejectionOfNullValueForNumber() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false).build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> b.field("type", "long"))).documentMapper();
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", (Object) null)))
        );
        assertThat(e.getCause().getMessage(), containsString("configured with [nullability=false] but were null"));
    }

    /**
     * A field-level {@code doc_values.nullability: false} (without relying on the index setting) causes enforcement: a document carrying
     * a null value for a keyword field is rejected with an {@link IllegalArgumentException} wrapped in {@link DocumentParsingException}.
     */
    public void testFieldLevelFalseEnforcesRejectionOfNullValue() throws Exception {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject())
        ).documentMapper();
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", (Object) null)))
        );
        assertThat(e.getCause().getMessage(), containsString("configured with [nullability=false] but were null"));
    }
}
