/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class DataStreamOptionsTests extends AbstractWireSerializingTestCase<DataStreamOptions> {

    @Override
    protected Writeable.Reader<DataStreamOptions> instanceReader() {
        return DataStreamOptions::read;
    }

    @Override
    protected DataStreamOptions createTestInstance() {
        return new DataStreamOptions(randomFrom(DataStreamOptions.FailureStore.values()));
    }

    @Override
    protected DataStreamOptions mutateInstance(DataStreamOptions instance) throws IOException {
        return new DataStreamOptions(
            randomValueOtherThan(instance.failureStore(), () -> randomFrom(DataStreamOptions.FailureStore.values()))
        );
    }
}
