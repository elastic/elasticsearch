/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.UpdateModelSnapshotAction.Response;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

public class PutModelSnapshotDescriptionActionResponseTests extends AbstractStreamableTestCase<UpdateModelSnapshotAction.Response> {

    @Override
    protected Response createTestInstance() {
        ModelSnapshot snapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));
        snapshot.setDescription(randomAsciiOfLengthBetween(1, 20));
        return new Response(snapshot);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
