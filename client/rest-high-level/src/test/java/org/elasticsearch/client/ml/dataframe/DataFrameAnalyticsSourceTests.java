/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static org.elasticsearch.client.ml.dataframe.QueryConfigTests.randomQueryConfig;


public class DataFrameAnalyticsSourceTests extends AbstractXContentTestCase<DataFrameAnalyticsSource> {

    public static DataFrameAnalyticsSource randomSourceConfig() {
        FetchSourceContext sourceFiltering = null;
        if (randomBoolean()) {
            sourceFiltering = new FetchSourceContext(true,
                generateRandomStringArray(10, 10, false, false),
                generateRandomStringArray(10, 10, false, false));
        }
        Map<String, Object> runtimeMappings = null;
        if (randomBoolean()) {
            runtimeMappings = new HashMap<>();
            Map<String, Object> runtimeField = new HashMap<>();
            runtimeField.put("type", "keyword");
            runtimeField.put("script", "");
            runtimeMappings.put(randomAlphaOfLength(10), runtimeField);
        }
        return DataFrameAnalyticsSource.builder()
            .setIndex(generateRandomStringArray(10, 10, false, false))
            .setQueryConfig(randomBoolean() ? null : randomQueryConfig())
            .setSourceFiltering(sourceFiltering)
            .setRuntimeMappings(runtimeMappings)
            .build();
    }

    @Override
    protected DataFrameAnalyticsSource doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsSource.fromXContent(parser);
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
    protected DataFrameAnalyticsSource createTestInstance() {
        return randomSourceConfig();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
