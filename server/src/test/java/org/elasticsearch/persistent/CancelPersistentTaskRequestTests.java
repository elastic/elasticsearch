/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.RemovePersistentTaskAction.Request;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;

public class CancelPersistentTaskRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAsciiOfLength(10));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }
}
