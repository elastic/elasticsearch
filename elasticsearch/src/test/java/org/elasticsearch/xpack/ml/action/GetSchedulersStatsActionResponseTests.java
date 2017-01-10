/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.GetSchedulersStatsAction.Response;
import org.elasticsearch.xpack.ml.job.persistence.QueryPage;
import org.elasticsearch.xpack.ml.scheduler.Scheduler;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

public class GetSchedulersStatsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Response.SchedulerStats> schedulerStatsList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String schedulerId = randomAsciiOfLength(10);
            SchedulerStatus schedulerStatus = randomFrom(SchedulerStatus.values());

            Response.SchedulerStats schedulerStats = new Response.SchedulerStats(schedulerId, schedulerStatus);
            schedulerStatsList.add(schedulerStats);
        }

        result = new Response(new QueryPage<>(schedulerStatsList, schedulerStatsList.size(), Scheduler.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
