/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.datasource.ASCIIStringsHandler;
import org.elasticsearch.datageneration.matchers.source.FlattenedFieldMatcher;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoaderTestRunner;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FlattenedFieldRootBlockLoaderTests extends BlockLoaderTestCase {

    public FlattenedFieldRootBlockLoaderTests(Params params) {
        super("flattened", List.of(new ASCIIStringsHandler()), params);
    }

    @Override
    protected BlockLoaderTestRunner configureRunner(BlockLoaderTestRunner runner, Settings.Builder settings, Mapping mapping) {
        return runner.matcher((expected, actual) -> {
            try {
                var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
                var matcher = new FlattenedFieldMatcher(mappingXContent, settings, mappingXContent, settings);

                List<Object> expectedList = parseExpected(expected);
                List<Object> actualList = parseActual(actual);

                var fieldMapping = mapping.lookup().get(runner.fieldName());

                var result = matcher.match(actualList, expectedList, fieldMapping, fieldMapping);
                assertTrue(result.getMessage(), result.isMatch());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private List<Object> parseExpected(Object expected) {
        return switch (expected) {
            case Map<?, ?> map -> List.of(map);
            case List<?> list -> (List<Object>) list;
            case null -> Collections.emptyList();
            default -> throw new IllegalArgumentException("Expected array or object, found " + expected.getClass().getSimpleName());
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> parseActual(Object actual) throws IOException {
        List<BytesRef> listActual = switch (actual) {
            case List<?> list -> (List<BytesRef>) actual;
            case BytesRef bytesRef -> List.of(bytesRef);
            case null -> Collections.emptyList();
            default -> throw new IllegalArgumentException("Expected array or BytesRef, found " + actual.getClass().getSimpleName());
        };

        return listActual.stream().map(bytesRef -> {
            try (
                XContentParser parser = XContentHelper.createParser(
                    XContentParserConfiguration.EMPTY,
                    new BytesArray(bytesRef),
                    XContentType.JSON
                )
            ) {
                var token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    return (Object) parser.map();
                } else {
                    throw new IllegalArgumentException("Expected object, found " + token);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).toList();
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        return value;
    }

    @Override
    public void testBlockLoaderOfMultiField() {
        assumeTrue("flattened fields do not support multi fields", false);
    }

}
