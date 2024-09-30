/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataStreamOptionsTests extends AbstractXContentSerializingTestCase<DataStreamOptions> {

    @Override
    protected Writeable.Reader<DataStreamOptions> instanceReader() {
        return DataStreamOptions::read;
    }

    @Override
    protected DataStreamOptions createTestInstance() {
        return new DataStreamOptions(randomBoolean() ? null : DataStreamFailureStoreTests.randomFailureStore());
    }

    @Override
    protected DataStreamOptions mutateInstance(DataStreamOptions instance) throws IOException {
        var failureStore = instance.failureStore();
        if (failureStore == null) {
            failureStore = DataStreamFailureStoreTests.randomFailureStore();
        } else {
            failureStore = randomBoolean() ? null : randomValueOtherThan(failureStore, DataStreamFailureStoreTests::randomFailureStore);
        }
        return new DataStreamOptions(failureStore);
    }

    @Override
    protected DataStreamOptions doParseInstance(XContentParser parser) throws IOException {
        return DataStreamOptions.fromXContent(parser);
    }
}
