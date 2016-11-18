/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetRecordsAction.Response;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

public class GetRecordsActionResponseTests extends AbstractStreamableTestCase<GetRecordsAction.Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<AnomalyRecord> hits = new ArrayList<>(listSize);
        String jobId = randomAsciiOfLengthBetween(1, 20);
        String bucketId = randomAsciiOfLengthBetween(1, 20);
        for (int j = 0; j < listSize; j++) {
            AnomalyRecord record = new AnomalyRecord(jobId);
            record.setId(randomAsciiOfLengthBetween(1, 20));
            hits.add(record);
        }
        QueryPage<AnomalyRecord> snapshots = new QueryPage<>(hits, listSize);
        return new Response(snapshots);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
