/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction.Response;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GetRecordsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

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
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
