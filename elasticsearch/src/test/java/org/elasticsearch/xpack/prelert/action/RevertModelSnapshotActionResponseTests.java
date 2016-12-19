/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.RevertModelSnapshotAction.Response;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

public class RevertModelSnapshotActionResponseTests extends AbstractStreamableTestCase<RevertModelSnapshotAction.Response> {

    @Override
    protected Response createTestInstance() {
        ModelSnapshot modelSnapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setDescription(randomAsciiOfLengthBetween(1, 20));
        return new RevertModelSnapshotAction.Response(modelSnapshot);
    }

    @Override
    protected Response createBlankInstance() {
        return new RevertModelSnapshotAction.Response();
    }

}
