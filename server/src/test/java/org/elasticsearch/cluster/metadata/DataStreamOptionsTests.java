/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataStreamOptionsTests extends AbstractXContentSerializingTestCase<DataStreamOptions> {

    @Override
    protected Writeable.Reader<DataStreamOptions> instanceReader() {
        return DataStreamOptions::new;
    }

    @Override
    protected DataStreamOptions createTestInstance() {
        return new DataStreamOptions(
            randomBoolean() ? null : DataStreamLifecycleTests.randomLifecycle(),
            randomBoolean() ? null : DataStreamFailureStoreTests.randomFailureStore()
        );
    }

    @Override
    protected DataStreamOptions mutateInstance(DataStreamOptions instance) throws IOException {
        var lifecycle = instance.lifecycle();
        var failureStore = instance.failureStore();
        if (randomBoolean()) {
            if (lifecycle == null) {
                lifecycle = DataStreamLifecycleTests.randomLifecycle();
            } else {
                lifecycle = randomBoolean() ? null : randomValueOtherThan(lifecycle, DataStreamLifecycleTests::randomLifecycle);
            }
        } else {
            if (failureStore == null) {
                failureStore = DataStreamFailureStoreTests.randomFailureStore();
            } else {
                failureStore = randomBoolean() ? null : new DataStreamFailureStore(failureStore.enabled() == false);
            }
        }
        return new DataStreamOptions(lifecycle, failureStore);
    }

    @Override
    protected DataStreamOptions doParseInstance(XContentParser parser) throws IOException {
        return DataStreamOptions.fromXContent(parser);
    }
}
