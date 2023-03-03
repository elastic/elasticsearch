/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.entsearch.engine.EngineTestUtils;

public class ListEnginesActionRequestSerializingTests extends AbstractWireSerializingTestCase<ListEnginesAction.Request> {

    @Override
    protected Writeable.Reader<ListEnginesAction.Request> instanceReader() {
        return ListEnginesAction.Request::new;
    }

    @Override
    protected ListEnginesAction.Request createTestInstance() {

        PageParams pageParams = EngineTestUtils.randomPageParams();
        String query = randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) });
        return new ListEnginesAction.Request(query, pageParams);
    }

    @Override
    protected ListEnginesAction.Request mutateInstance(ListEnginesAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
