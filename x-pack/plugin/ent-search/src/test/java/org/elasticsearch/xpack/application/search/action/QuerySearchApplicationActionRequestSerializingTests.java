/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;

import java.io.IOException;

public class QuerySearchApplicationActionRequestSerializingTests extends AbstractWireSerializingTestCase<
    QuerySearchApplicationAction.Request> {

    @Override
    protected Writeable.Reader<QuerySearchApplicationAction.Request> instanceReader() {
        return QuerySearchApplicationAction.Request::new;
    }

    @Override
    protected QuerySearchApplicationAction.Request createTestInstance() {
        return new QuerySearchApplicationAction.Request(
            randomAlphaOfLengthBetween(1, 10),
            SearchApplicationTestUtils.randomSearchApplicationQueryParams()
        );
    }

    @Override
    protected QuerySearchApplicationAction.Request mutateInstance(QuerySearchApplicationAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

}
