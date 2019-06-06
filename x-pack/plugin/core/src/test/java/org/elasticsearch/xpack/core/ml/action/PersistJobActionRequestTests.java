/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PersistJobActionRequestTests  extends AbstractWireSerializingTestCase<PersistJobAction.Request> {

    @Override
    protected Writeable.Reader<PersistJobAction.Request> instanceReader() {
        return PersistJobAction.Request::new;
    }

    @Override
    protected PersistJobAction.Request createTestInstance() {
        return new PersistJobAction.Request(randomAlphaOfLength(10));
    }
}
