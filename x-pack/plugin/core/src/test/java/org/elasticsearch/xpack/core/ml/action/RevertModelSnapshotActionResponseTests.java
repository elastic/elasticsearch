/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction.Response;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotTests;

public class RevertModelSnapshotActionResponseTests extends AbstractWireSerializingTestCase<RevertModelSnapshotAction.Response> {

    @Override
    protected Response createTestInstance() {
        return new RevertModelSnapshotAction.Response(ModelSnapshotTests.createRandomized());
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
