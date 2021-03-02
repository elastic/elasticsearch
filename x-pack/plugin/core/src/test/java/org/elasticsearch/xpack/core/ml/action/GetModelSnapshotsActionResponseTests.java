/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction.Response;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotTests;

import java.util.ArrayList;
import java.util.List;

public class GetModelSnapshotsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<ModelSnapshot> hits = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            hits.add(ModelSnapshotTests.createRandomized());
        }
        QueryPage<ModelSnapshot> snapshots = new QueryPage<>(hits, listSize, ModelSnapshot.RESULTS_FIELD);
        return new Response(snapshots);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
