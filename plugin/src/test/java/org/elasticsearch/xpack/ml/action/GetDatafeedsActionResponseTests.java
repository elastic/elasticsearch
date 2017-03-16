/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.GetDatafeedsAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

public class GetDatafeedsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<DatafeedConfig> datafeedList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            datafeedList.add(DatafeedConfigTests.createRandomizedDatafeedConfig(randomAsciiOfLength(10)));
        }
        return new Response(new QueryPage<>(datafeedList, datafeedList.size(), DatafeedConfig.RESULTS_FIELD));
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }
}
