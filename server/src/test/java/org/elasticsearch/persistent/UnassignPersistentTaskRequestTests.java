/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;

public class UnassignPersistentTaskRequestTests extends AbstractWireSerializingTestCase<UnassignPersistentTaskAction.Request> {

    @Override
    protected UnassignPersistentTaskAction.Request createTestInstance() {
        return new UnassignPersistentTaskAction.Request(randomAsciiLettersOfLength(10), randomNonNegativeLong(),
            randomAsciiLettersOfLength(50));
    }

    @Override
    protected Writeable.Reader<UnassignPersistentTaskAction.Request> instanceReader() {
        return UnassignPersistentTaskAction.Request::new;
    }
}
