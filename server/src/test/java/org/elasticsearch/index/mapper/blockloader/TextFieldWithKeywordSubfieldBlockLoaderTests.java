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

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.datasource.MultifieldAddonHandler;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoaderTestRunner;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.BlockLoaderTestCase.buildSpecification;

/**
 * Exercises the strict-columnar dedup path: a text parent that carries a keyword multi-field. When that keyword sub-field is a complete,
 * byte-identical copy of the raw values the parent skips its own doc values and loads through the delegate; otherwise it keeps its own.
 */
public class TextFieldWithKeywordSubfieldBlockLoaderTests extends MapperServiceTestCase {
    private final BlockLoaderTestCase.Params params;
    private final BlockLoaderTestRunner runner;

    @ParametersFactory(argumentFormatting = "preference=%s")
    public static List<Object[]> args() {
        return BlockLoaderTestCase.args();
    }

    public TextFieldWithKeywordSubfieldBlockLoaderTests(BlockLoaderTestCase.Params params) {
        this.params = params;
        this.runner = new BlockLoaderTestRunner(params).breaker(newLimitedBreaker(ByteSizeValue.ofMb(1)));
        if (randomBoolean()) {
            runner.allowDummyDocs();
        }
    }

    public void testBlockLoaderOfTextFieldWithKeywordSubfield() throws IOException {
        // The dedup this exercises - skipping the text field's own doc values in favour of a plain keyword delegate - only happens in
        // strict-columnar mode. In other modes the text field has no own doc values anyway, so there is nothing new to assert here.
        assumeTrue(
            "dedup of text doc values against a keyword sub-field only applies in strict-columnar mode",
            params.indexMode().isStrictColumnar()
        );

        var template = new Template(Map.of("parent", new Template.Leaf("parent", FieldType.TEXT.toString())));
        var specification = buildSpecification(
            List.of(new MultifieldAddonHandler(Map.of(FieldType.TEXT, List.of(FieldType.KEYWORD)), 1f)),
            params.indexMode()
        );

        var mapping = new MappingGenerator(specification).generate(template);
        var fieldMapping = mapping.lookup().get("parent");

        runner.document(new DocumentGenerator(specification).generate(template, mapping));
        var fieldValue = runner.mapDoc().get("parent");

        Object expected = TextFieldBlockLoaderTests.expectedValue(
            fieldMapping,
            fieldValue,
            params,
            new BlockLoaderTestCase.TestContext(false, false),
            false
        );
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        runner.mapperService(mapperServiceForParams(mappingXContent));
        runner.fieldName("parent");
        runner.run(expected);
    }

    private MapperService mapperServiceForParams(XContentBuilder mappingXContent) throws IOException {
        // Columnar index modes preserve array order via offsets recorded on the field's own doc values, so the index must actually be built
        // in columnar mode for those offsets to exist - otherwise the source-order expectation no longer holds.
        if (params.indexMode().isColumnar()) {
            return createMapperService(BlockLoaderTestCase.getSettingsForParams(params).build(), mappingXContent);
        }
        return params.syntheticSource() ? createSytheticSourceMapperService(mappingXContent) : createMapperService(mappingXContent);
    }
}
