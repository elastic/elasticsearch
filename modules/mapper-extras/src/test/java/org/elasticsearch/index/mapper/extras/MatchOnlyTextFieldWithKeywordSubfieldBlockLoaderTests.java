/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.datasource.MultifieldAddonHandler;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoaderTestRunner;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.BlockLoaderTestCase.buildSpecification;

/**
 * Exercises the strict-columnar dedup path for match_only_text: a parent that carries a keyword multi-field. Whether the parent keeps its
 * own doc values or delegates to a plain keyword copy, the loaded values come back in source order, so both branches share an expectation.
 */
public class MatchOnlyTextFieldWithKeywordSubfieldBlockLoaderTests extends MapperServiceTestCase {
    private final BlockLoaderTestCase.Params params;
    private final BlockLoaderTestRunner runner;

    @ParametersFactory(argumentFormatting = "preference=%s")
    public static List<Object[]> args() {
        return BlockLoaderTestCase.args();
    }

    public MatchOnlyTextFieldWithKeywordSubfieldBlockLoaderTests(BlockLoaderTestCase.Params params) {
        this.params = params;
        this.runner = new BlockLoaderTestRunner(params).breaker(newLimitedBreaker(ByteSizeValue.ofMb(1)));
        if (randomBoolean()) {
            runner.allowDummyDocs();
        }
    }

    public void testBlockLoaderOfMatchOnlyTextFieldWithKeywordSubfield() throws IOException {
        // The dedup this exercises - skipping the field's own doc values in favour of a plain keyword delegate - only happens in
        // strict-columnar mode. In other modes the field has no own doc values anyway, so there is nothing new to assert here.
        assumeTrue(
            "dedup of doc values against a keyword sub-field only applies in strict-columnar mode",
            params.indexMode().isStrictColumnar()
        );

        var template = new Template(Map.of("parent", new Template.Leaf("parent", FieldType.MATCH_ONLY_TEXT.toString())));
        var specification = buildSpecification(
            List.of(new MultifieldAddonHandler(Map.of(FieldType.MATCH_ONLY_TEXT, List.of(FieldType.KEYWORD)), 1f)),
            params.indexMode()
        );

        var mapping = new MappingGenerator(specification).generate(template);
        runner.document(new DocumentGenerator(specification).generate(template, mapping));
        var fieldValue = runner.mapDoc().get("parent");

        // Either the field keeps its own multi-valued doc values (read via the offsets sidecar) or it delegates to a plain keyword copy
        // with no normalizer/ignore_above/null_value transforming the values - both yield the non-null values in source order.
        Object expected = valuesInSourceOrder(fieldValue);
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        runner.mapperService(createMapperService(BlockLoaderTestCase.getSettingsForParams(params).build(), mappingXContent));
        runner.fieldName("parent");
        runner.run(expected);
    }

    @SuppressWarnings("unchecked")
    private static Object valuesInSourceOrder(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return new BytesRef(s);
        }
        var resultList = ((List<String>) value).stream().filter(Objects::nonNull).map(BytesRef::new).toList();
        if (resultList.isEmpty()) {
            return null;
        }
        if (resultList.size() == 1) {
            return resultList.get(0);
        }
        return resultList;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }
}
