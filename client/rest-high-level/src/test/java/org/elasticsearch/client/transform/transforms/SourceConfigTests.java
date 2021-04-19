/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;


public class SourceConfigTests extends AbstractXContentTestCase<SourceConfig> {

    public static SourceConfig randomSourceConfig() {
        return new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            QueryConfigTests.randomQueryConfig(),
            randomRuntimeMappings());
    }

    private static Map<String, Object> randomRuntimeMappings() {
        return randomList(0, 10, () -> randomAlphaOfLengthBetween(1, 10)).stream()
            .distinct()
            .collect(toMap(f -> f, f -> singletonMap("type", randomFrom("boolean", "date", "double", "keyword", "long"))));
    }

    @Override
    protected SourceConfig doParseInstance(XContentParser parser) throws IOException {
        return SourceConfig.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only as QueryConfig stores a Map<String, Object>
        return field -> field.isEmpty() == false;
    }

    @Override
    protected SourceConfig createTestInstance() {
        return randomSourceConfig();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
