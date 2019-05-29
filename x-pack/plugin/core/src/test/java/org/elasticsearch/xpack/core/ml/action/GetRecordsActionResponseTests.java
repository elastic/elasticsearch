/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction.Response;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GetRecordsActionResponseTests extends AbstractStreamableTestCase<GetRecordsAction.Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<AnomalyRecord> hits = new ArrayList<>(listSize);
        String jobId = randomAlphaOfLengthBetween(1, 20);
        for (int j = 0; j < listSize; j++) {
            AnomalyRecord record = new AnomalyRecord(jobId, new Date(), 600);
            hits.add(record);
        }
        QueryPage<AnomalyRecord> snapshots = new QueryPage<>(hits, listSize, AnomalyRecord.RESULTS_FIELD);
        return new Response(snapshots);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
