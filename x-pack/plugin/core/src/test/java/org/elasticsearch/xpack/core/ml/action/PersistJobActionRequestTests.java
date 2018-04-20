/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class PersistJobActionRequestTests  extends AbstractStreamableTestCase<PersistJobAction.Request> {
    @Override
    protected PersistJobAction.Request createBlankInstance() {
        return new PersistJobAction.Request();
    }

    @Override
    protected PersistJobAction.Request createTestInstance() {
        return new PersistJobAction.Request(randomAlphaOfLength(10));
    }
}
