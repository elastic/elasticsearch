/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;

public class DataFrameAnalyticsSourceTests extends AbstractSerializingTestCase<DataFrameAnalyticsSource> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected DataFrameAnalyticsSource doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsSource.createParser(false).apply(parser, null);
    }

    @Override
    protected DataFrameAnalyticsSource createTestInstance() {
        return createRandom();
    }

    public static DataFrameAnalyticsSource createRandom() {
        String[] index = generateRandomStringArray(10, 10, false, false);
        QueryProvider queryProvider = null;
        if (randomBoolean()) {
            try {
                queryProvider = QueryProvider.fromParsedQuery(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
            } catch (IOException e) {
                // Should never happen
                throw new UncheckedIOException(e);
            }
        }
        return new DataFrameAnalyticsSource(index, queryProvider);
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsSource> instanceReader() {
        return DataFrameAnalyticsSource::new;
    }
}
