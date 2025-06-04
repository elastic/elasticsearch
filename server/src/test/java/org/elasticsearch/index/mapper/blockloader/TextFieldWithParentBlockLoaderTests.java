/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoaderTestRunner;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.BlockLoaderTestCase.buildSpecification;
import static org.elasticsearch.index.mapper.BlockLoaderTestCase.hasDocValues;

public class TextFieldWithParentBlockLoaderTests extends MapperServiceTestCase {
    private final BlockLoaderTestCase.Params params;
    private final BlockLoaderTestRunner runner;

    @ParametersFactory(argumentFormatting = "preference=%s")
    public static List<Object[]> args() {
        return BlockLoaderTestCase.args();
    }

    public TextFieldWithParentBlockLoaderTests(BlockLoaderTestCase.Params params) {
        this.params = params;
        this.runner = new BlockLoaderTestRunner(params);
    }

    // This is similar to BlockLoaderTestCase#testBlockLoaderOfMultiField but has customizations required to properly test the case
    // of text multi field in a keyword field.
    public void testBlockLoaderOfParentField() throws IOException {
        var template = new Template(Map.of("parent", new Template.Leaf("parent", FieldType.KEYWORD.toString())));
        DataGeneratorSpecification specification = buildSpecification(List.of(new DataSourceHandler() {
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

                    var keywordParentMapping = dataSource.get(
                        new DataSourceRequest.LeafMappingParametersGenerator(
                            dataSource,
                            "_field",
                            FieldType.KEYWORD.toString(),
                            request.eligibleCopyToFields(),
                            request.dynamicMapping()
                        )
                    ).mappingGenerator().get();

                    var textMultiFieldMapping = dataSource.get(
                        new DataSourceRequest.LeafMappingParametersGenerator(
                            dataSource,
                            "_field",
                            FieldType.TEXT.toString(),
                            request.eligibleCopyToFields(),
                            request.dynamicMapping()
                        )
                    ).mappingGenerator().get();

                    // we don't need this here
                    keywordParentMapping.remove("copy_to");

                    textMultiFieldMapping.put("type", "text");
                    textMultiFieldMapping.remove("fields");

                    keywordParentMapping.put("fields", Map.of("mf", textMultiFieldMapping));

                    return keywordParentMapping;
                });
            }
        }));
        var mapping = new MappingGenerator(specification).generate(template);
        var fieldMapping = mapping.lookup().get("parent");

        var document = new DocumentGenerator(specification).generate(template, mapping);
        var fieldValue = document.get("parent");

        Object expected = expected(fieldMapping, fieldValue, new BlockLoaderTestCase.TestContext(false, true));
        XContentBuilder mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        var mapperService = params.syntheticSource()
            ? createSytheticSourceMapperService(mappingXContent)
            : createMapperService(mappingXContent);

        runner.runTest(mapperService, document, expected, "parent.mf");
    }

    @SuppressWarnings("unchecked")
    private Object expected(Map<String, Object> fieldMapping, Object value, BlockLoaderTestCase.TestContext testContext) {
        assert fieldMapping.containsKey("fields");

        Object normalizer = fieldMapping.get("normalizer");
        boolean docValues = hasDocValues(fieldMapping, true);
        boolean store = fieldMapping.getOrDefault("store", false).equals(true);

        // if text sub field is stored, then always use that:
        var textFieldMapping = (Map<String, Object>) ((Map<String, Object>) fieldMapping.get("fields")).get("mf");
        if (textFieldMapping.getOrDefault("store", false).equals(true)) {
            return TextFieldBlockLoaderTests.expectedValue(textFieldMapping, value, params, testContext);
        }

        if (docValues || store) {
            // we are using block loader of the parent field
            return KeywordFieldBlockLoaderTests.expectedValue(fieldMapping, value, params, testContext);
        }

        // we are using block loader of the text field itself
        return TextFieldBlockLoaderTests.expectedValue(textFieldMapping, value, params, testContext);
    }
}
