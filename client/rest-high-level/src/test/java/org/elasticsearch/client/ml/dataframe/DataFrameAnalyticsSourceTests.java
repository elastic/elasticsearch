/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static org.elasticsearch.client.ml.dataframe.QueryConfigTests.randomQueryConfig;


public class DataFrameAnalyticsSourceTests extends AbstractXContentTestCase<DataFrameAnalyticsSource> {

    public static DataFrameAnalyticsSource randomSourceConfig() {
        return DataFrameAnalyticsSource.builder()
            .setIndex(generateRandomStringArray(10, 10, false, false))
            .setQueryConfig(randomBoolean() ? null : randomQueryConfig())
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
        return field -> !field.isEmpty();
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
