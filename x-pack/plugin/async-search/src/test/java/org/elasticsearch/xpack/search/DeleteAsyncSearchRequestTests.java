/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;

import static org.elasticsearch.xpack.search.GetAsyncSearchRequestTests.randomSearchId;

public class DeleteAsyncSearchRequestTests extends AbstractWireSerializingTestCase<DeleteAsyncSearchAction.Request> {
    @Override
    protected Writeable.Reader<DeleteAsyncSearchAction.Request> instanceReader() {
        return DeleteAsyncSearchAction.Request::new;
    }

    @Override
    protected DeleteAsyncSearchAction.Request createTestInstance() {
        return new DeleteAsyncSearchAction.Request(randomSearchId());
    }
}
