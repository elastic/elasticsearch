/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.CompletionPersistentTaskAction.Request;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class CompletionPersistentTaskRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        if (randomBoolean()) {
            return new Request(randomAlphaOfLength(10), randomNonNegativeLong(), null, null);
        } else {
            return new Request(randomAlphaOfLength(10), randomNonNegativeLong(), null, randomAlphaOfLength(20));
        }
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }
}
