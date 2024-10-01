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

import static org.hamcrest.Matchers.containsString;

public class DataStreamFailureStoreTests extends AbstractXContentSerializingTestCase<DataStreamFailureStore> {

    @Override
    protected Writeable.Reader<DataStreamFailureStore> instanceReader() {
        return DataStreamFailureStore::read;
    }

    @Override
    protected DataStreamFailureStore createTestInstance() {
        return randomFailureStore();
    }

    @Override
    protected DataStreamFailureStore mutateInstance(DataStreamFailureStore instance) throws IOException {
        // We know the enabled is not null because in this test we do not include the DataStreamFailureStore.NULL
        return new DataStreamFailureStore(instance.enabled() == false);
    }

    @Override
    protected DataStreamFailureStore doParseInstance(XContentParser parser) throws IOException {
        return DataStreamFailureStore.fromXContent(parser);
    }

    static DataStreamFailureStore randomFailureStore() {
        return new DataStreamFailureStore(randomBoolean());
    }

    public void testInvalidEmptyConfiguration() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DataStreamFailureStore(null));
        assertThat(exception.getMessage(), containsString("at least one non-null configuration value"));
    }
}
