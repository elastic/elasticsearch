/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.xpack.persistent.CompletionPersistentTaskAction.Request;
import org.elasticsearch.test.AbstractStreamableTestCase;

public class RestartPersistentTaskRequestTests extends AbstractStreamableTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomLong(), null);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}