/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetDataFrameAnalyticsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<DataFrameAnalyticsConfig> analytics = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            analytics.add(DataFrameAnalyticsConfigTests.createRandom(DataFrameAnalyticsConfigTests.randomValidId()));
        }
        return new Response(new QueryPage<>(analytics, analytics.size(), Response.RESULTS_FIELD));
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }
}
