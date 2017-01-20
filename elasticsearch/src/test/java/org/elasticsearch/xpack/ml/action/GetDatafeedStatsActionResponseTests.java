/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction.Response;
import org.elasticsearch.xpack.ml.job.persistence.QueryPage;
import org.elasticsearch.xpack.ml.datafeed.Datafeed;
import org.elasticsearch.xpack.ml.datafeed.DatafeedStatus;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

public class GetDatafeedStatsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Response.DatafeedStats> datafeedStatsList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String datafeedId = randomAsciiOfLength(10);
            DatafeedStatus datafeedStatus = randomFrom(DatafeedStatus.values());

            Response.DatafeedStats datafeedStats = new Response.DatafeedStats(datafeedId, datafeedStatus);
            datafeedStatsList.add(datafeedStats);
        }

        result = new Response(new QueryPage<>(datafeedStatsList, datafeedStatsList.size(), Datafeed.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
