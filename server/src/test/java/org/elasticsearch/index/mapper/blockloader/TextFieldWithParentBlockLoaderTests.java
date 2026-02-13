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

import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.datasource.MultifieldAddonHandler;
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
        if (randomBoolean()) {
            runner.allowDummyDocs();
        }
    }

    // This is similar to BlockLoaderTestCase#testBlockLoaderOfMultiField but has customizations required to properly test the case
    // of text multi field in a keyword field.
    public void testBlockLoaderOfParentField() throws IOException {
        var template = new Template(Map.of("parent", new Template.Leaf("parent", FieldType.KEYWORD.toString())));
        var specification = buildSpecification(List.of(new MultifieldAddonHandler(Map.of(FieldType.KEYWORD, List.of(FieldType.TEXT)), 1f)));

        var mapping = new MappingGenerator(specification).generate(template);
        var fieldMapping = mapping.lookup().get("parent");

        runner.document(new DocumentGenerator(specification).generate(template, mapping));
        var fieldValue = runner.mapDoc().get("parent");

        Object expected = expected(fieldMapping, fieldValue, new BlockLoaderTestCase.TestContext(false, true));
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        runner.mapperService(
            params.syntheticSource() ? createSytheticSourceMapperService(mappingXContent) : createMapperService(mappingXContent)
        );
        runner.fieldName("parent.subfield_text");
        runner.run(expected);
    }

    @SuppressWarnings("unchecked")
    private Object expected(Map<String, Object> fieldMapping, Object value, BlockLoaderTestCase.TestContext testContext) {
        assert fieldMapping.containsKey("fields");

        Object normalizer = fieldMapping.get("normalizer");
        boolean docValues = hasDocValues(fieldMapping, true);
        boolean store = fieldMapping.getOrDefault("store", false).equals(true);

        if (normalizer == null && (docValues || store)) {
            // we are using block loader of the parent field
            return KeywordFieldBlockLoaderTests.expectedValue(fieldMapping, value, params, testContext);
        }

        // we are using block loader of the text field itself
        var textFieldMapping = (Map<String, Object>) ((Map<String, Object>) fieldMapping.get("fields")).get("subfield_text");
        return TextFieldBlockLoaderTests.expectedValue(textFieldMapping, value, params, testContext, false);
    }
}
