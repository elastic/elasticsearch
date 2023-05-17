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
import org.elasticsearch.xpack.core.action.util.PageParams;

public class ListSearchApplicationActionRequestSerializingTests extends AbstractWireSerializingTestCase<
    ListSearchApplicationAction.Request> {

    @Override
    protected Writeable.Reader<ListSearchApplicationAction.Request> instanceReader() {
        return ListSearchApplicationAction.Request::new;
    }

    @Override
    protected ListSearchApplicationAction.Request createTestInstance() {

        PageParams pageParams = SearchApplicationTestUtils.randomPageParams();
        String query = randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) });
        return new ListSearchApplicationAction.Request(query, pageParams);
    }

    @Override
    protected ListSearchApplicationAction.Request mutateInstance(ListSearchApplicationAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
