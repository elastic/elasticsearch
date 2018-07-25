/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class PersistJobActionResponseTests extends AbstractStreamableTestCase<PersistJobAction.Response> {
    @Override
    protected PersistJobAction.Response createBlankInstance() {
        return new PersistJobAction.Response();
    }

    @Override
    protected PersistJobAction.Response createTestInstance() {
        return new PersistJobAction.Response(randomBoolean());
    }
}
