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

public class PutSearchApplicationActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutSearchApplicationAction.Request> {

    @Override
    protected Writeable.Reader<PutSearchApplicationAction.Request> instanceReader() {
        return PutSearchApplicationAction.Request::new;
    }

    @Override
    protected PutSearchApplicationAction.Request createTestInstance() {
        return new PutSearchApplicationAction.Request(SearchApplicationTestUtils.randomSearchApplication(), randomBoolean());
    }

    @Override
    protected PutSearchApplicationAction.Request mutateInstance(PutSearchApplicationAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
