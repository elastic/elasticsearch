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
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocValuesParameterTests extends MapperServiceTestCase {

    // -----------------------------------------------------------------------
    // Columnar invariant: every field's _source must be reconstructable from doc-value columns, since columnar rebuilds
    // _source purely from them and keeps no generic source fallback. A field whose synthetic source is FALLBACK (no doc
    // values, like doc_values:false, or a type whose doc-value encoding can't rebuild its own source) is rejected on a
    // root field; multi-fields (never in _source) are exempt.
    // -----------------------------------------------------------------------

    public void testColumnarEnabledFalseObjectIsLossy() throws IOException {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("meta");
            b.field("type", "object");
            b.field("enabled", false);
            b.endObject();
        })).documentMapper();
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("meta");
            b.field("a", "x");
            b.field("b", 1);
            b.endObject();
        }));
        // The disabled subtree is dropped (lossy) in columnar: it is neither stored as _ignored_source nor given any
        // doc values, so columnar never needs the generic source fallback for it.
        for (org.apache.lucene.index.IndexableField f : doc.rootDoc().getFields()) {
            assertNotEquals("disabled object stored as _ignored_source: " + f.name(), IgnoredSourceFieldMapper.NAME, f.name());
            assertFalse("field materialized from disabled subtree: " + f.name(), f.name().startsWith("meta"));
        }
    }

    public void testColumnarRejectsDocValuesFalseOnRootField() {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(settings, fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "field [field] cannot reconstruct _source from doc values; "
                    + "every field must be reconstructable from doc values in index using [columnar]"
            )
        );
    }

    public void testColumnarAllowsDocValuesFalseOnMultiField() throws IOException {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // A search-only multi-field may opt out of doc values: it never appears in _source, so it needs no columnar
        // representation (the user accepts they can't aggregate on it).
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                b.startObject("raw").field("type", "keyword").field("doc_values", false).endObject();
                b.endObject();
            }
            b.endObject();
        }));
        assertTrue(mapperService.fieldType("field").hasDocValues());
        assertFalse(mapperService.fieldType("field.raw").hasDocValues());
    }

    public void testColumnarAllowsSparseVectorByDefault() throws IOException {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // sparse_vector reconstructs _source from a stored field; store defaults to true, so it is reconstructable and
        // allowed in columnar.
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "sparse_vector")));
        assertTrue("store should default to true for sparse_vector", mapperService.fieldType("field").isStored());
    }

    public void testColumnarRejectsSparseVectorWithoutStore() {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // With store:false sparse_vector has no native synthetic source, so its _source cannot be reconstructed and it
        // is rejected.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(settings, fieldMapping(b -> b.field("type", "sparse_vector").field("store", false)))
        );
        assertThat(e.getMessage(), containsString("field [field] cannot reconstruct _source from doc values"));
    }

    public void testColumnarAllowsDocValuesEmptyMap() throws IOException {
        assumeTrue("feature under test must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // The map form of doc_values (even empty) means "doc values enabled", so doc_values:{} resolves to enabled and
        // the field is accepted in columnar rather than treated as disabled.
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("doc_values").endObject();
        }));
        assertTrue(mapperService.fieldType("field").hasDocValues());
    }

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
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
        );
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
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
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
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
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
}
