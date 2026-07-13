/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexAction.Request;

public class ReindexDatastreamIndexRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(20));
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return new ReindexDataStreamIndexAction.Request(randomValueOtherThan(instance.getSourceIndex(), () -> randomAlphaOfLength(20)));
    }
}
