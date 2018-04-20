/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction.Response;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotTests;

public class RevertModelSnapshotActionResponseTests extends AbstractStreamableTestCase<RevertModelSnapshotAction.Response> {

    @Override
    protected Response createTestInstance() {
        return new RevertModelSnapshotAction.Response(ModelSnapshotTests.createRandomized());
    }

    @Override
    protected Response createBlankInstance() {
        return new RevertModelSnapshotAction.Response();
    }

}
