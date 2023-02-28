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
import org.elasticsearch.xpack.entsearch.engine.Engine;
import org.elasticsearch.xpack.entsearch.engine.EngineTestUtils;

public class ListEnginesActionResponseSerializingTests extends AbstractWireSerializingTestCase<ListEnginesAction.Response> {

    @Override
    protected Writeable.Reader<ListEnginesAction.Response> instanceReader() {
        return ListEnginesAction.Response::new;
    }

    @Override
    protected ListEnginesAction.Response createTestInstance() {
        PageParams pageParams = EngineTestUtils.randomPageParams();
        return new ListEnginesAction.Response(
            randomArray(10, Engine[]::new, EngineTestUtils::randomEngine),
            pageParams,
            randomIntBetween(0, 1000)
        );
    }

    @Override
    protected ListEnginesAction.Response mutateInstance(ListEnginesAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
