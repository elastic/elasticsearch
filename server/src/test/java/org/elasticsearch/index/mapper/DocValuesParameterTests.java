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
import static org.hamcrest.Matchers.notNullValue;

public class DocValuesParameterTests extends MapperServiceTestCase {

    // -----------------------------------------------------------------------
    // Columnar invariant: every field's _source must be reconstructable from doc-value columns, since columnar rebuilds
    // _source purely from them and keeps no generic source fallback. A field whose synthetic source is FALLBACK (no doc
    // values, like doc_values:false, or a type whose doc-value encoding can't rebuild its own source) is rejected on a
    // root field; multi-fields (never in _source) are exempt.
    // -----------------------------------------------------------------------

    public void testColumnarEnabledFalseObjectIsLossy() throws IOException {
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
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // sparse_vector reconstructs _source from a stored field; store defaults to true, so it is reconstructable and
        // allowed in columnar.
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "sparse_vector")));
        assertTrue("store should default to true for sparse_vector", mapperService.fieldType("field").isStored());
    }

    public void testColumnarRejectsSparseVectorWithoutStore() {
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
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, false, true))
        );
    }

    // -----------------------------------------------------------------------
    // Boolean shorthand: doc_values: true / false still works alongside map form
    // -----------------------------------------------------------------------

    public void testDocValuesTrueShorthand() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", true)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(true));
    }

    public void testDocValuesFalseShorthand() throws Exception {
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
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword")));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, false, true))
        );
    }

    /**
     * When the index setting is {@code false} and the field does not set its own {@code doc_values.multi_value}, the field resolves to
     * single-valued.
     */
    public void testIndexSettingFalseDefaultsNumberToSingleValued() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "long")));
        NumberFieldMapper mapper = (NumberFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false, true))
        );
    }

    /**
     * The {@code index.mapping.doc_values.multi_value} and {@code index.mapping.doc_values.nullability} index-level settings are
     * only honoured in strict-columnar index modes, just like the per-field {@code doc_values} object form. Outside strict-columnar
     * mode, fields default to multi-valued and nullable regardless of these settings.
     */
    public void testIndexSettingsIgnoredOutsideColumnarMode() throws Exception {
        Settings settings = Settings.builder()
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false)
            .build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword")));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, true, true))
        );
        // a multi-valued document is accepted, and a missing/null value is accepted, despite both index settings being false
        mapperService.documentMapper().parse(source(b -> b.array("field", "a", "b")));
        mapperService.documentMapper().parse(source(b -> {}));
    }

    /**
     * A field-level {@code doc_values.multi_value: true} overrides the index setting of {@code false}, keeping the field multi-valued.
     */
    public void testFieldLevelTrueOverridesIndexSettingFalse() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
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
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), true)
            .build();
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
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
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
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
        DocumentMapper mapper = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", true).endObject())
        ).documentMapper();
        // must not throw
        mapper.parse(source(b -> b.array("field", randomAlphanumericOfLength(4), randomAlphanumericOfLength(4))));
    }

    public void testMultiValueRejectedInNonColumnarMode() throws Exception {
        for (boolean multiValue : new boolean[] { false, true }) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", multiValue).endObject())
                )
            );
            assertThat(e.getMessage(), containsString("unsupported doc_values configuration"));
            assertThat(e.getMessage(), containsString("supported values: [true, false]"));
        }
    }

    // -----------------------------------------------------------------------
    // nullability
    // -----------------------------------------------------------------------

    public void testNullabilityRejectedInNonColumnarMode() throws Exception {
        for (boolean nullability : new boolean[] { false, true }) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", nullability).endObject())
                )
            );
            assertThat(e.getMessage(), containsString("unsupported doc_values configuration"));
            assertThat(e.getMessage(), containsString("supported values: [true, false]"));
        }
    }

    public void testNullabilityFalseParsedFromMapForm() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().nullability(), equalTo(false));
        assertThat(mapper.isNullable(), equalTo(false));
    }

    public void testIndexSettingNullabilityFalseRequiresValue() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false)
            .build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword"))).documentMapper();
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {})));
        assertThat(e.getMessage(), containsString("configured with [nullability=false] but no value was provided"));
    }

    public void testDynamicFieldDoesNotMaskMissingRequiredField() throws Exception {
        // With the index-level setting, a dynamically-mapped field inherits nullability=false and marks itself satisfied. It must not
        // count toward the statically-required "field": a document supplying only the dynamic field is still rejected for missing "field".
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false)
            .build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword"))).documentMapper();
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("other", randomAlphanumericOfLength(5))))
        );
        assertThat(e.getMessage(), containsString("[field]"));
        assertThat(e.getMessage(), containsString("nullability=false"));
    }

    public void testFastPathSizeCheckRejectsAndAcceptsWithoutDynamicMappers() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // No dynamic fields are created here, so enforcement takes the O(1) size-check fast path. Two static required fields.
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("a").field("type", "keyword").startObject("doc_values").field("nullability", false).endObject().endObject();
            b.startObject("b").field("type", "keyword").startObject("doc_values").field("nullability", false).endObject().endObject();
        })).documentMapper();
        // both present => sizes match => accepted
        mapper.parse(source(b -> b.field("a", randomAlphanumericOfLength(5)).field("b", randomAlphanumericOfLength(5))));
        // one missing => size mismatch on the fast path => rejected, naming the missing field
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("a", randomAlphanumericOfLength(5))))
        );
        assertThat(e.getMessage(), containsString("[b]"));
        assertThat(e.getMessage(), containsString("nullability=false"));
    }

    public void testDynamicIntroductionUsesContainmentThenFastPathOnLaterDocuments() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false)
            .build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "keyword")));
        // doc1 introduces dynamic numeric "dyn": hasDynamicMappers() is true, so enforcement uses containment. "field" is present so the
        // doc
        // is accepted, and the stray dynamic "dyn" (absent from the pre-doc required set) does not trigger a false rejection on that path.
        int dynValue = randomInt();
        var doc1 = mapperService.documentMapper().parse(source(b -> b.field("field", "a").field("dyn", dynValue)));
        assertThat(doc1.dynamicMappingsUpdate(), notNullValue());
        mergeDynamicUpdate(mapperService, doc1.dynamicMappingsUpdate());
        // "dyn" is now a static required field. A later doc that omits it creates no dynamic mapper, so the size-check fast path runs and,
        // because "dyn" is now in the required set, correctly rejects the document.
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapperService.documentMapper().parse(source(b -> b.field("field", "b")))
        );
        assertThat(e.getMessage(), containsString("[dyn]"));
        // a later doc carrying both required fields is accepted on the fast path
        mapperService.documentMapper().parse(source(b -> b.field("field", "c").field("dyn", randomInt())));
    }

    public void testFieldLevelNullabilityTrueOverridesIndexSettingFalse() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false)
            .build();
        DocumentMapper mapper = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", true).endObject())
        ).documentMapper();
        // must not throw: the field opts back into accepting missing values
        mapper.parse(source(b -> {}));
    }

    public void testNullabilityIsSealedAgainstUpdate() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        // false -> true is rejected
        MapperService sealedFalse = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject())
        );
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                sealedFalse,
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", true).endObject())
            )
        );
        assertThat(e1.getMessage(), containsString("Cannot update parameter [doc_values]"));
        // true -> false is also rejected
        MapperService startTrue = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", true).endObject())
        );
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                startTrue,
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject())
            )
        );
        assertThat(e2.getMessage(), containsString("Cannot update parameter [doc_values]"));
    }

    public void testNullabilityFalseExemptedByNullValue() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(settings, fieldMapping(b -> {
            b.field("type", "keyword").field("null_value", "NA");
            b.startObject("doc_values").field("nullability", false).endObject();
        })).documentMapper();
        // null_value defined => field is never required: both missing and explicit null are accepted.
        mapper.parse(source(b -> {}));
        mapper.parse(source(b -> b.nullField("field")));
    }

    public void testNullabilityFalseNestedEnforcedPerInstance() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("a");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("b").field("type", "integer").startObject("doc_values").field("nullability", false).endObject().endObject();
            b.endObject();
            b.endObject();
        })).documentMapper();

        // every instance carries b => accepted
        mapper.parse(
            source(b -> b.startArray("a").startObject().field("b", 1).endObject().startObject().field("b", 2).endObject().endArray())
        );
        // one instance missing b => rejected (per-instance, not satisfied by the sibling)
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("a").startObject().field("b", 1).endObject().startObject().endObject().endArray()))
        );
        assertThat(e.getMessage(), containsString("[a.b]"));
        assertThat(e.getMessage(), containsString("nullability=false"));
        // absent nested array => zero Lucene docs => vacuously satisfied
        mapper.parse(source(b -> {}));
        // empty nested array => zero instances => accepted
        mapper.parse(source(b -> b.startArray("a").endArray()));
        // a single empty nested object => one instance with no b => rejected
        expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.startObject("a").endObject())));
    }

    /**
     * A scalar {@code [nullability=false]} field gets checked on the single root Lucene doc, so arrays that yield no value are rejected: an
     * empty array and an all-null array both mark nothing, while any array carrying at least one non-null value satisfies that requirement.
     */
    public void testNullabilityFalseRejectsEmptyAndAllNullArraysButAcceptsValueArray() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(
            settings,
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("nullability", false).endObject())
        ).documentMapper();
        // empty array => loop body never runs => nothing marked => rejected, same as a missing field
        DocumentParsingException empty = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").endArray()))
        );
        assertThat(empty.getMessage(), containsString("[field]"));
        assertThat(empty.getMessage(), containsString("nullability=false"));
        // array of only nulls => each null routes past the value mark => still rejected
        DocumentParsingException nulls = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.array("field", new Object[] { null, null })))
        );
        assertThat(nulls.getMessage(), containsString("[field]"));
        // at least one non-null value marks the field satisfied => accepted
        mapper.parse(source(b -> b.array("field", new Object[] { null, randomAlphanumericOfLength(5) })));
    }
}
