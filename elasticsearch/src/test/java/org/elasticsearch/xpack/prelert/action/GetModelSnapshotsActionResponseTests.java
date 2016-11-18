/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetModelSnapshotsAction.Response;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

public class GetModelSnapshotsActionResponseTests extends AbstractStreamableTestCase<GetModelSnapshotsAction.Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<ModelSnapshot> hits = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            ModelSnapshot snapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));
            snapshot.setDescription(randomAsciiOfLengthBetween(1, 20));
            hits.add(snapshot);
        }
        QueryPage<ModelSnapshot> snapshots = new QueryPage<>(hits, listSize);
        return new Response(snapshots);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
