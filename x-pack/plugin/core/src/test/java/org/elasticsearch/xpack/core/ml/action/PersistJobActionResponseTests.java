/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PersistJobActionResponseTests extends AbstractWireSerializingTestCase<PersistJobAction.Response> {
    @Override
    protected Writeable.Reader<PersistJobAction.Response> instanceReader() {
        return PersistJobAction.Response::new;
    }

    @Override
    protected PersistJobAction.Response createTestInstance() {
        return new PersistJobAction.Response(randomBoolean());
    }

    @Override
    protected PersistJobAction.Response mutateInstance(PersistJobAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
