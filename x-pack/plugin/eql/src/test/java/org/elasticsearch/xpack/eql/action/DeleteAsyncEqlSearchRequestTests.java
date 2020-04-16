/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.eql.action.DeleteAsyncEqlSearchAction;

import static org.elasticsearch.xpack.eql.action.GetAsyncEqlSearchRequestTests.randomSearchId;


public class DeleteAsyncEqlSearchRequestTests extends AbstractWireSerializingTestCase<DeleteAsyncEqlSearchAction.Request> {
    @Override
    protected Writeable.Reader<DeleteAsyncEqlSearchAction.Request> instanceReader() {
        return DeleteAsyncEqlSearchAction.Request::new;
    }

    @Override
    protected DeleteAsyncEqlSearchAction.Request createTestInstance() {
        return new DeleteAsyncEqlSearchAction.Request(randomSearchId());
    }
}
