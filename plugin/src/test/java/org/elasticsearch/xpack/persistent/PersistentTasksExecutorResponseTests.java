/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class PersistentTasksExecutorResponseTests extends AbstractStreamableTestCase<PersistentTaskResponse> {

    @Override
    protected PersistentTaskResponse createTestInstance() {
        return new PersistentTaskResponse(randomLong());
    }

    @Override
    protected PersistentTaskResponse createBlankInstance() {
        return new PersistentTaskResponse();
    }
}