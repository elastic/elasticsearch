/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PersistJobActionRequestTests extends AbstractWireSerializingTestCase<PersistJobAction.Request> {

    @Override
    protected Writeable.Reader<PersistJobAction.Request> instanceReader() {
        return PersistJobAction.Request::new;
    }

    @Override
    protected PersistJobAction.Request createTestInstance() {
        return new PersistJobAction.Request(randomAlphaOfLength(10));
    }

    @Override
    protected PersistJobAction.Request mutateInstance(PersistJobAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
