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
import org.elasticsearch.datageneration.datasource.DefaultMappingParametersHandler;
import org.elasticsearch.datageneration.datasource.DefaultObjectGenerationHandler;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Verifies that a {@code columnar_stored} source index and an equivalent {@code synthetic} source index
 * (both using {@code columnar} index mode) return identical {@code _source} for every document.
 */
public class SyntheticVersusColumnarStoredSourceIT extends ESIntegTestCase {

    @Before
    public void checkFeatureFlag() {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

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

    private void runTest(boolean useTimeSeriesDocValuesFormat) throws Exception {
        var spec = buildSpec();
        var template = new TemplateGenerator(spec).generate();
        var mapping = new MappingGenerator(spec).generate(template);
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping.raw());

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

        var document = new DocumentGenerator(spec).generate(template, mapping);
        logger.info("mappings: {}", Strings.toString(mappingXContent));
        logger.info("document: {}", document);
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
            .withNestedFieldsLimit(0)
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
            }, new DefaultMappingParametersHandler() {
                @Override
                public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
                    var delegated = super.handle(request);
                    if (delegated == null) {
                        return null;
                    }
                    return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                        var mapping = new HashMap<>(delegated.mappingGenerator().get());
                        // synthetic_source_keep is not allowed in columnar index mode
                        mapping.remove(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM);
                        mapping.remove("store");
                        mapping.remove("copy_to");
                        return mapping;
                    });
                }
            }))
            .build();
    }
}
