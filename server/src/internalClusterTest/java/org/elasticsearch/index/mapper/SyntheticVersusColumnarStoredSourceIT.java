/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.datasource.ASCIIStringsHandler;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.datasource.DefaultObjectGenerationHandler;
import org.elasticsearch.datageneration.datasource.MultifieldAddonHandler;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Verifies that a {@code columnar_stored} source index and an equivalent {@code synthetic} source index
 * (both using {@code columnar} index mode) return identical {@code _source} for every document.
 */
public class SyntheticVersusColumnarStoredSourceIT extends ESIntegTestCase {

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        // Columnar mode requires DOC_VALUES_ONLY for seq_no; remove the randomly-chosen value so
        // it doesn't conflict with the index-mode default (disable_sequence_numbers=true).
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    public void testDocValuesIgnoredSource() throws Exception {
        runTest(true);
    }

    public void testStoredIgnoredSource() throws Exception {
        runTest(false);
    }

    /**
     * A multi-field may keep {@code doc_values:false} in columnar mode (it is exempt from the doc-values contract since it never
     * appears in {@code _source}). Verify both source modes accept such a mapping and reconstruct identical {@code _source}.
     */
    public void testMultiFieldWithoutDocValues() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("message")
            .field("type", "text")
            .startObject("fields")
            .startObject("raw")
            .field("type", "keyword")
            .field("doc_values", false)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertEqualSource(mappingXContent, Map.of("message", "foo bar"), randomBoolean());
    }

    /**
     * A dynamic field inside a nested object must be mapped inside the nested mapper rather than flattened to a root leaf (the
     * columnar default is {@code subobjects:false}). Indexing such a document used to never converge its dynamic mapping and trip
     * the noop-mapping-update retry guard. Verify the document indexes and both source modes reconstruct identical {@code _source}.
     */
    public void testDynamicFieldInsideNested() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("n")
            .field("type", "nested")
            .startObject("properties")
            .startObject("leaf")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        // 'extra' is not in the mapping, so it is dynamically mapped inside the nested object.
        var document = Map.of("n", List.of(Map.of("leaf", "a", "extra", "x"), Map.of("leaf", "b", "extra", "y")));
        assertEqualSource(mappingXContent, document, randomBoolean());
    }

    /**
     * The mapping declares a single level of nesting ({@code n}), which columnar supports. An object sub-field
     * that appears as an array of objects ({@code obj}) is materialized under {@code subobjects:false} as its own child
     * documents, parented to the {@code n} child - a second <em>physical</em> level of documents in the Lucene block,
     * even though no second {@code nested} field is declared.
     */
    public void testNestedWithObjectArraySubfield() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("n")
            .field("type", "nested")
            .startObject("properties")
            .startObject("obj")
            .startObject("properties")
            .startObject("val")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        var document = Map.of("n", List.of(Map.of("obj", List.of(Map.of("val", 1), Map.of("val", 2)))));
        assertEqualSource(mappingXContent, document, randomBoolean());
    }

    /**
     * With the time-series doc-values format disabled, a keyword sub-field of a nested field whose values exceed
     * {@code ignore_above} keeps a copy of the ignored values in a per-document stored field so synthetic source can reconstruct
     * them. Each nested array entry is a separate Lucene document, but columnar_stored reconstructs them all through one reused
     * single-document reader that always reports the same doc id, and a reused stored-field loader skips re-reading on an unchanged
     * doc id - so it used to return the first entry's stored values for every sibling. Verify every entry keeps its own values.
     */
    public void testNestedWithIgnoredKeywordPerEntry() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("n")
            .field("type", "nested")
            .startObject("properties")
            .startObject("kw")
            .field("type", "keyword")
            .field("ignore_above", 4)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        // Every value exceeds ignore_above (length 4), so all are stored in the ignored-value fallback. The first entry is
        // multi-valued to make first-entry-duplication observable if the reused stored-field loader is not read per entry.
        var document = Map.of("n", List.of(Map.of("kw", List.of("aaaaa", "bbbbb")), Map.of("kw", "ccccc"), Map.of("kw", "ddddd")));
        // The stored-field fallback (and thus this bug) only occurs with the time-series doc-values format disabled.
        assertEqualSource(mappingXContent, document, false);
    }

    /**
     * A {@code multi_value=false, on_failure=ignore} field that receives two values redirects the extra value to the
     * {@code ._on_failure} column. Both source modes must reconstruct the same document: {@code synthetic} reads the first
     * value from its doc-values column and the second from {@code ._on_failure}; {@code columnar_stored} reads it from the
     * whole-document blob written before the column is pruned.
     */
    public void testMultiValueViolationRestoredIdenticallyAcrossSourceModes() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("kw")
            .field("type", "keyword")
            .startObject("doc_values")
            .field("multi_value", false)
            .field("on_failure", "ignore")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        // Two values: "a" is indexed normally, "b" is redirected to ._on_failure. Both must appear in _source.
        assertEqualSource(mappingXContent, Map.of("kw", List.of("a", "b")), randomBoolean());
        for (String index : List.of("test_synthetic", "test_columnar_stored")) {
            assertIgnoredContains(index, "kw");
        }
    }

    /**
     * A {@code nullability=false, on_failure=ignore} field that is absent in a document is merely marked ignored rather
     * than rejecting the document. Both source modes must omit the field from the reconstructed {@code _source}.
     */
    public void testNullabilityViolationOmittedIdenticallyAcrossSourceModes() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("kw")
            .field("type", "keyword")
            .startObject("doc_values")
            .field("nullability", false)
            .field("on_failure", "ignore")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        // field is absent — violation is ignored, field must be omitted from _source in both modes
        assertEqualSource(mappingXContent, Map.of(), randomBoolean());
        for (String index : List.of("test_synthetic", "test_columnar_stored")) {
            assertIgnoredContains(index, "kw");
        }
    }

    /**
     * Asserts reconstructed sources are the same and that the field is stored in _ignored across both source types when normalizer is used.
     */
    public void testFallbackMultiValueViolationRestoredIdenticallyAcrossSourceModes() throws Exception {
        var mappingXContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("kw")
            .field("type", "keyword")
            .field("normalizer", "lowercase")
            .startObject("doc_values")
            .field("multi_value", false)
            .field("on_failure", "ignore")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertEqualSource(mappingXContent, Map.of("kw", List.of("HELLO", "WORLD")), randomBoolean());
        for (String index : List.of("test_synthetic", "test_columnar_stored")) {
            assertIgnoredContains(index, "kw");
        }
    }

    private void assertIgnoredContains(String index, String fieldName) {
        var resp = client().prepareSearch(index).addFetchField(IgnoredFieldMapper.NAME).get();
        try {
            var ignoredField = resp.getHits().getAt(0).field(IgnoredFieldMapper.NAME);
            assertNotNull(index + ": violation must populate _ignored", ignoredField);
            assertTrue(index + ": _ignored must contain '" + fieldName + "'", ignoredField.getValues().contains(fieldName));
        } finally {
            resp.decRef();
        }
    }

    private void runTest(boolean useTimeSeriesDocValuesFormat) throws Exception {
        var spec = buildSpec();
        var template = new TemplateGenerator(spec).generate();
        var mapping = new MappingGenerator(spec).generate(template);
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping.raw());
        var document = new DocumentGenerator(spec).generate(template, mapping);
        logger.info("mappings: {}", Strings.toString(mappingXContent));
        logger.info("document: {}", document);
        assertEqualSource(mappingXContent, document, useTimeSeriesDocValuesFormat);
    }

    private void assertEqualSource(XContentBuilder mappingXContent, Map<String, ?> document, boolean useTimeSeriesDocValuesFormat) {
        var syntheticSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.toString())
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), useTimeSeriesDocValuesFormat)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        var columnarStoredSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), useTimeSeriesDocValuesFormat)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);

        assertAcked(prepareCreate("test_synthetic").setMapping(mappingXContent).setSettings(syntheticSettings));
        assertAcked(prepareCreate("test_columnar_stored").setMapping(mappingXContent).setSettings(columnarStoredSettings));

        prepareIndex("test_synthetic").setId("1").setSource(document).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("test_columnar_stored").setId("1").setSource(document).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        var syntheticSource = client().prepareGet("test_synthetic", "1").get().getSourceAsMap();
        var columnarStoredSource = client().prepareGet("test_columnar_stored", "1").get().getSourceAsMap();

        assertEquals(syntheticSource, columnarStoredSource);
    }

    private DataGeneratorSpecification buildSpec() {
        var allowedFieldTypes = DefaultObjectGenerationHandler.ALLOWED_FIELD_TYPES.stream()
            // these types require plugins not loaded in server internalClusterTests
            .filter(
                ft -> ft != FieldType.WILDCARD
                    && ft != FieldType.COUNTED_KEYWORD
                    && ft != FieldType.CONSTANT_KEYWORD
                    && ft != FieldType.SCALED_FLOAT
                    && ft != FieldType.MATCH_ONLY_TEXT
            )
            .toList();
        return DataGeneratorSpecification.builder()
            .withMaxFieldCountPerLevel(5)
            .withMaxObjectDepth(2)
            // Allow a single nested field so the equivalence check also covers nested reconstruction across both source modes.
            // The limit of 1 means at most one nested field total, so the generator can never produce nested-inside-nested, which
            // columnar rejects (columnar supports only a single level of nesting).
            .withNestedFieldsLimit(1)
            .withDataSourceHandlers(List.of(new ASCIIStringsHandler()))
            .withDataSourceHandlers(List.of(new DataSourceHandler() {
                @Override
                public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                    return new DataSourceResponse.FieldTypeGenerator(
                        () -> new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(randomFrom(allowedFieldTypes).toString())
                    );
                }

                @Override
                public DataSourceResponse.ObjectMappingParametersGenerator handle(
                    DataSourceRequest.ObjectMappingParametersGenerator request
                ) {
                    // columnar mode does not support the subobjects mapping parameter
                    return new DataSourceResponse.ObjectMappingParametersGenerator(HashMap::new);
                }
            },
                // Randomly attach a string multi-field (text<->keyword) to string fields, exercising columnar source
                // equivalence with multi-fields present. Only TEXT/KEYWORD are used since the other string types are
                // filtered out above.
                new MultifieldAddonHandler(Map.of(FieldType.TEXT, List.of(FieldType.KEYWORD), FieldType.KEYWORD, List.of(FieldType.TEXT)))
            ))
            .withIndexMode(IndexMode.COLUMNAR)
            .build();
    }
}
