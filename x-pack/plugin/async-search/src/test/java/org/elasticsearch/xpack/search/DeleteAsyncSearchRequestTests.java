/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;

import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;

public class DeleteAsyncSearchRequestTests extends AbstractWireSerializingTestCase<DeleteAsyncResultRequest> {
    @Override
    protected Writeable.Reader<DeleteAsyncResultRequest> instanceReader() {
        return DeleteAsyncResultRequest::new;
    }

    @Override
    protected DeleteAsyncResultRequest createTestInstance() {
        return new DeleteAsyncResultRequest(randomSearchId());
    }

    @Override
    protected DeleteAsyncResultRequest mutateInstance(DeleteAsyncResultRequest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
