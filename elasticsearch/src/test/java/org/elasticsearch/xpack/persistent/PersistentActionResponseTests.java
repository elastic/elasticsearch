/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class PersistentActionResponseTests extends AbstractStreamableTestCase<PersistentActionResponse> {

    @Override
    protected PersistentActionResponse createTestInstance() {
        return new PersistentActionResponse(randomLong());
    }

    @Override
    protected PersistentActionResponse createBlankInstance() {
        return new PersistentActionResponse();
    }
}