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

public class GetSearchApplicationActionResponseSerializingTests extends AbstractWireSerializingTestCase<
    GetSearchApplicationAction.Response> {

    @Override
    protected Writeable.Reader<GetSearchApplicationAction.Response> instanceReader() {
        return GetSearchApplicationAction.Response::new;
    }

    @Override
    protected GetSearchApplicationAction.Response createTestInstance() {
        return new GetSearchApplicationAction.Response(SearchApplicationTestUtils.randomSearchApplication());
    }

    @Override
    protected GetSearchApplicationAction.Response mutateInstance(GetSearchApplicationAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
