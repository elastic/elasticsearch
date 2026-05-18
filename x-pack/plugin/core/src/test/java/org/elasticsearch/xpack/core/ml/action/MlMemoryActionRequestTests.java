/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class MlMemoryActionRequestTests extends AbstractWireSerializingTestCase<MlMemoryAction.Request> {

    @Override
    protected Writeable.Reader<MlMemoryAction.Request> instanceReader() {
        return MlMemoryAction.Request::new;
    }

    @Override
    protected MlMemoryAction.Request createTestInstance() {
        return new MlMemoryAction.Request(randomAlphaOfLength(20));
    }

    @Override
    protected MlMemoryAction.Request mutateInstance(MlMemoryAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
