/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
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
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasItem;

public class ColumnarSourceIT extends ESIntegTestCase {

    @Before
    public void checkFeatureFlag() throws Exception {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        // Columnar mode requires DOC_VALUES_ONLY for seq_no; remove the randomly-chosen value so
        // it doesn't conflict with the index-mode default (disable_sequence_numbers=true).
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    /**
     * Verifies that in {@code columnar_stored} source mode, every non-null field in an indexed document is stored in
     * {@code _ignored_source}. A random mapping and document are generated each run to exercise a wide variety of field
     * types and nesting shapes.
     */
    public void testAllFieldsStoredInIgnoredSource() throws Exception {
        var spec = buildSpec();
        var template = new TemplateGenerator(spec).generate();
        var mapping = new MappingGenerator(spec).generate(template);

        var indexMode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        var settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), indexMode.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();

        var mappingXContent = XContentFactory.jsonBuilder().map(mapping.raw());
        assertAcked(prepareCreate("test").setMapping(mappingXContent).setSettings(settings));

        var document = new DocumentGenerator(spec).generate(template, mapping);
        logger.info("mappings: {}", Strings.toString(mappingXContent));
        logger.info("document: {}", document);
        prepareIndex("test").setSource(document).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        flushAndRefresh("test");

        var ignoredSourceFieldNames = readIgnoredSourceFieldNames();

        var expectedFieldNames = collectNonNullLeafPaths(document, "", mapping.lookup());
        for (String fieldName : expectedFieldNames) {
            assertThat("field '" + fieldName + "' should be stored in _ignored_source", ignoredSourceFieldNames, hasItem(fieldName));
        }
    }

    /**
     * Verifies that {@code columnar_stored} and {@code synthetic} source modes produce an identical source document
     * when retrieving an indexed document. Also checks that the returned source is flat, i.e., all values are scalar
     * (no nested object values) and fields are addressed by their full dotted path.
     */
    public void testColumnarStoredSourceMatchesSyntheticSource() throws Exception {
        var spec = buildSpec();
        var template = new TemplateGenerator(spec).generate();
        var mapping = new MappingGenerator(spec).generate(template);
        var mappingXContent = XContentFactory.jsonBuilder().map(mapping.raw());

        var indexMode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        var syntheticSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), indexMode.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.toString())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        var columnarStoredSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), indexMode.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
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

        MatchResult matchResult = Matcher.matchSource()
            .mappings(mapping.lookup(), mappingXContent, mappingXContent)
            .settings(columnarStoredSettings, syntheticSettings)
            .expected(List.of(syntheticSource))
            .ignoringSort(true)
            .isEqualTo(List.of(columnarStoredSource));
        assertTrue(matchResult.getMessage(), matchResult.isMatch());

        // A Map value is only acceptable for leaf field types that use an object representation
        // (e.g. geo_point stored as {lat, lon}). Object mappers must not appear — those should
        // have been flattened to dotted-path keys.
        for (var entry : columnarStoredSource.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?>) {
                assertTrue(
                    "source should be flat, but key '" + entry.getKey() + "' has a nested object value",
                    isMappedLeafField(entry.getKey(), mapping.lookup())
                );
            }
        }
        for (var entry : syntheticSource.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?>) {
                assertTrue(
                    "source should be flat, but key '" + entry.getKey() + "' has a nested object value",
                    isMappedLeafField(entry.getKey(), mapping.lookup())
                );
            }
        }
    }

    private Set<String> readIgnoredSourceFieldNames() throws IOException {
        var clusterState = clusterService().state();
        var primaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        var primaryNodeName = clusterState.nodes().get(primaryShard.currentNodeId()).getName();

        var indicesService = internalCluster().getInstance(IndicesService.class, primaryNodeName);
        var indexService = indicesService.indexServiceSafe(clusterState.metadata().getProject().index("test").getIndex());
        var indexShard = indexService.getShard(0);

        assert IgnoredSourceFieldMapper.ignoredSourceFormat(
            indexService.getIndexSettings()
        ) == IgnoredSourceFieldMapper.IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE;

        return collectIgnoredSourceFieldNames(indexShard);
    }

    private Set<String> collectIgnoredSourceFieldNames(IndexShard shard) throws IOException {
        Set<String> fieldNames = new HashSet<>();
        try (var searcher = shard.acquireSearcher("test_verify")) {
            var reader = searcher.getDirectoryReader();
            for (LeafReaderContext ctx : reader.leaves()) {
                var leafReader = ctx.reader();
                var docValues = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, IgnoredSourceFieldMapper.NAME);
                for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                    Map<String, List<Object>> storedFieldsMap = new HashMap<>();
                    var storedDoc = leafReader.storedFields().document(docId);
                    for (var field : storedDoc.getFields(IgnoredSourceFieldMapper.NAME)) {
                        storedFieldsMap.computeIfAbsent(IgnoredSourceFieldMapper.NAME, k -> new ArrayList<>()).add(field.binaryValue());
                    }
                    var ignoredFields = IgnoredSourceFieldMapper.IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE.loadIgnoredFields(
                        null,
                        storedFieldsMap,
                        docId,
                        docValues
                    );
                    for (var entry : ignoredFields.entrySet()) {
                        for (var nv : entry.getValue()) {
                            fieldNames.add(nv.name());
                        }
                    }
                }
            }
        }
        return fieldNames;
    }

    @SuppressWarnings("unchecked")
    private Set<String> collectNonNullLeafPaths(Map<String, Object> doc, String prefix, Map<String, Map<String, Object>> mappingLookup) {
        Set<String> paths = new HashSet<>();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String fullPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                // null values are not indexed, skip
            } else if (value instanceof Map<?, ?> nested) {
                if (isMappedLeafField(fullPath, mappingLookup)) {
                    // A mapped leaf field with an object-shaped value (e.g. geo_point stored as {lat, lon}).
                    // _ignored_source stores the whole field under its mapped path, not its sub-keys.
                    paths.add(fullPath);
                } else {
                    paths.addAll(collectNonNullLeafPaths((Map<String, Object>) nested, fullPath, mappingLookup));
                }
            } else if (value instanceof List<?> list) {
                // Find the first non-null element; skip all-null arrays (not stored in _ignored_source).
                list.stream().filter(Objects::nonNull).findFirst().ifPresent(firstNonNull -> {
                    if (firstNonNull instanceof Map<?, ?> && isMappedLeafField(fullPath, mappingLookup) == false) {
                        // array of objects under an object-mapper or unmapped path: recurse into each item
                        list.stream().filter(item -> item instanceof Map<?, ?>).forEach(item -> {
                            paths.addAll(collectNonNullLeafPaths((Map<String, Object>) item, fullPath, mappingLookup));
                        });
                    } else {
                        // array of scalars, or array of objects for a mapped leaf field (e.g. geo_point)
                        paths.add(fullPath);
                    }
                });
            } else {
                paths.add(fullPath);
            }
        }
        return paths;
    }

    private static boolean isMappedLeafField(String fullPath, Map<String, Map<String, Object>> mappingLookup) {
        Map<String, Object> fieldMapping = mappingLookup.get(fullPath);
        if (fieldMapping == null) {
            return false;
        }
        String type = (String) fieldMapping.get("type");
        return "object".equals(type) == false && "nested".equals(type) == false;
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
                        return mapping;
                    });
                }
            }))
            .build();
    }
}
