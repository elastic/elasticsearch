/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class BlockLoaderTestCase extends MapperServiceTestCase {
    private static final MappedFieldType.FieldExtractPreference[] PREFERENCES = new MappedFieldType.FieldExtractPreference[] {
        MappedFieldType.FieldExtractPreference.NONE,
        MappedFieldType.FieldExtractPreference.DOC_VALUES,
        MappedFieldType.FieldExtractPreference.STORED };

    @ParametersFactory(argumentFormatting = "preference=%s")
    public static List<Object[]> args() {
        List<Object[]> args = new ArrayList<>();
        for (boolean syntheticSource : new boolean[] { false, true }) {
            for (MappedFieldType.FieldExtractPreference preference : PREFERENCES) {
                args.add(new Object[] { new Params(syntheticSource, preference) });
            }
        }
        return args;
    }

    public record Params(boolean syntheticSource, MappedFieldType.FieldExtractPreference preference) {}

    public record TestContext(boolean forceFallbackSyntheticSource, boolean isMultifield) {}

    private final String fieldType;
    protected final Params params;
    private final Collection<DataSourceHandler> customDataSourceHandlers;
    private final BlockLoaderTestRunner runner;

    private final String fieldName;

    protected BlockLoaderTestCase(String fieldType, Params params) {
        this(fieldType, List.of(), params);
    }

    protected BlockLoaderTestCase(String fieldType, Collection<DataSourceHandler> customDataSourceHandlers, Params params) {
        this.fieldType = fieldType;
        this.params = params;
        this.customDataSourceHandlers = customDataSourceHandlers;
        this.runner = new BlockLoaderTestRunner(params);

        this.fieldName = randomAlphaOfLengthBetween(5, 10);
    }

    @Override
    public void testFieldHasValue() {
        assumeTrue("random test inherited from MapperServiceTestCase", false);
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assumeTrue("random test inherited from MapperServiceTestCase", false);
    }

    public void testBlockLoader() throws IOException {
        var template = new Template(Map.of(fieldName, new Template.Leaf(fieldName, fieldType)));
        var specification = buildSpecification(customDataSourceHandlers);

        var mapping = new MappingGenerator(specification).generate(template);
        var document = new DocumentGenerator(specification).generate(template, mapping);

        Object expected = expected(mapping.lookup().get(fieldName), getFieldValue(document, fieldName), new TestContext(false, false));

        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        var mapperService = params.syntheticSource
            ? createSytheticSourceMapperService(mappingXContent)
            : createMapperService(mappingXContent);

        runner.runTest(mapperService, document, expected, fieldName);
    }

    @SuppressWarnings("unchecked")
    public void testBlockLoaderForFieldInObject() throws IOException {
        int depth = randomIntBetween(0, 3);

        Map<String, Template.Entry> currentLevel = new HashMap<>();
        Map<String, Template.Entry> top = Map.of("top", new Template.Object("top", false, currentLevel));

        var fullFieldName = new StringBuilder("top");
        int currentDepth = 0;
        while (currentDepth++ < depth) {
            fullFieldName.append('.').append("level").append(currentDepth);

            Map<String, Template.Entry> nextLevel = new HashMap<>();
            currentLevel.put("level" + currentDepth, new Template.Object("level" + currentDepth, false, nextLevel));
            currentLevel = nextLevel;
        }

        fullFieldName.append('.').append(fieldName);
        currentLevel.put(fieldName, new Template.Leaf(fieldName, fieldType));
        var template = new Template(top);

        var specification = buildSpecification(customDataSourceHandlers);
        var mapping = new MappingGenerator(specification).generate(template);
        var document = new DocumentGenerator(specification).generate(template, mapping);

        TestContext testContext = new TestContext(false, false);

        if (params.syntheticSource && randomBoolean()) {
            // force fallback synthetic source in the hierarchy
            var docMapping = (Map<String, Object>) mapping.raw().get("_doc");
            var topLevelMapping = (Map<String, Object>) ((Map<String, Object>) docMapping.get("properties")).get("top");
            topLevelMapping.put("synthetic_source_keep", "all");

            testContext = new TestContext(true, false);
        }

        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        var mapperService = params.syntheticSource
            ? createSytheticSourceMapperService(mappingXContent)
            : createMapperService(mappingXContent);

        Object expected = expected(
            mapping.lookup().get(fullFieldName.toString()),
            getFieldValue(document, fullFieldName.toString()),
            testContext
        );

        runner.runTest(mapperService, document, expected, fullFieldName.toString());
    }

    @SuppressWarnings("unchecked")
    public void testBlockLoaderOfMultiField() throws IOException {
        // We are going to have a parent field and a multi field of the same type in order to be sure we can index data.
        // Then we'll test block loader of the multi field.
        var template = new Template(Map.of("parent", new Template.Leaf("parent", fieldType)));

        var customHandlers = new ArrayList<DataSourceHandler>();
        customHandlers.add(new DataSourceHandler() {
            @Override
            public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
                // This is a bit tricky meta-logic.
                // We want to customize mapping but to do this we need the mapping for the same field type
                // so we use name to untangle this.
                if (request.fieldName().equals("parent") == false) {
                    return null;
                }

                return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                    var dataSource = request.dataSource();

                    // We need parent field to have the same mapping as multi field due to different behavior caused f.e. by
                    // ignore_malformed.
                    // The name here should be different from "parent".
                    var mapping = dataSource.get(
                        new DataSourceRequest.LeafMappingParametersGenerator(
                            dataSource,
                            "_field",
                            request.fieldType(),
                            request.eligibleCopyToFields(),
                            request.dynamicMapping()
                        )
                    ).mappingGenerator().get();

                    var parentMapping = new HashMap<>(mapping);
                    var multiFieldMapping = new HashMap<>(mapping);

                    multiFieldMapping.put("type", fieldType);
                    multiFieldMapping.remove("fields");

                    parentMapping.put("fields", Map.of("mf", multiFieldMapping));

                    return parentMapping;
                });
            }
        });
        customHandlers.addAll(customDataSourceHandlers);
        var specification = buildSpecification(customHandlers);
        var mapping = new MappingGenerator(specification).generate(template);
        var fieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.lookup().get("parent").get("fields")).get("mf");

        var document = new DocumentGenerator(specification).generate(template, mapping);

        Object expected = expected(fieldMapping, getFieldValue(document, "parent"), new TestContext(false, true));
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        var mapperService = params.syntheticSource
            ? createSytheticSourceMapperService(mappingXContent)
            : createMapperService(mappingXContent);

        runner.runTest(mapperService, document, expected, "parent.mf");
    }

    public static DataGeneratorSpecification buildSpecification(Collection<DataSourceHandler> customHandlers) {
        return DataGeneratorSpecification.builder()
            .withFullyDynamicMapping(false)
            // Disable dynamic mapping and disabled objects
            .withDataSourceHandlers(List.of(new DataSourceHandler() {
                @Override
                public DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
                    return new DataSourceResponse.DynamicMappingGenerator(isObject -> false);
                }

                @Override
                public DataSourceResponse.ObjectMappingParametersGenerator handle(
                    DataSourceRequest.ObjectMappingParametersGenerator request
                ) {
                    return new DataSourceResponse.ObjectMappingParametersGenerator(HashMap::new); // just defaults
                }
            }))
            .withDataSourceHandlers(customHandlers)
            .build();
    }

    protected abstract Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext);

    protected static Object maybeFoldList(List<?> list) {
        if (list.isEmpty()) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        return list;
    }

    protected Object getFieldValue(Map<String, Object> document, String fieldName) {
        var rawValues = new ArrayList<>();
        processLevel(document, fieldName, rawValues);

        if (rawValues.size() == 1) {
            return rawValues.get(0);
        }

        return rawValues.stream().flatMap(v -> v instanceof List<?> l ? l.stream() : Stream.of(v)).toList();
    }

    @SuppressWarnings("unchecked")
    private void processLevel(Map<String, Object> level, String field, ArrayList<Object> values) {
        if (field.contains(".") == false) {
            var value = level.get(field);
            values.add(value);
            return;
        }

        var nameInLevel = field.split("\\.")[0];
        var entry = level.get(nameInLevel);
        if (entry instanceof Map<?, ?> m) {
            processLevel((Map<String, Object>) m, field.substring(field.indexOf('.') + 1), values);
        }
        if (entry instanceof List<?> l) {
            for (var object : l) {
                processLevel((Map<String, Object>) object, field.substring(field.indexOf('.') + 1), values);
            }
        }
    }

    public static boolean hasDocValues(Map<String, Object> fieldMapping, boolean defaultValue) {
        return (boolean) fieldMapping.getOrDefault("doc_values", defaultValue);
    }
}
