/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ResetJobRequestTests extends AbstractWireSerializingTestCase<ResetJobAction.Request> {

    @Override
    protected ResetJobAction.Request createTestInstance() {
        ResetJobAction.Request request = new ResetJobAction.Request(randomAlphaOfLength(10));
        request.setShouldStoreResult(randomBoolean());
        request.setSkipJobStateValidation(randomBoolean());
        return request;
    }

    @Override
    protected ResetJobAction.Request mutateInstance(ResetJobAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<ResetJobAction.Request> instanceReader() {
        return ResetJobAction.Request::new;
    }
}
