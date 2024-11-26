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
        return DataStreamFailureStore::new;
    }

    @Override
    protected DataStreamFailureStore createTestInstance() {
        return randomFailureStore();
    }

    @Override
    protected DataStreamFailureStore mutateInstance(DataStreamFailureStore instance) throws IOException {
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
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DataStreamFailureStore((Boolean) null));
        assertThat(exception.getMessage(), containsString("at least one non-null configuration value"));
    }
}
