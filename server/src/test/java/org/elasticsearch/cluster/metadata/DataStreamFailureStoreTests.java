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
    protected DataStreamFailureStore mutateInstance(DataStreamFailureStore instance) {
        var enabled = instance.enabled();
        var lifecycle = instance.lifecycle();
        switch (randomIntBetween(0, 1)) {
            case 0 -> enabled = enabled != null && lifecycle != null && randomBoolean() ? null : Boolean.FALSE.equals(enabled);
            case 1 -> lifecycle = lifecycle != null && enabled != null && randomBoolean()
                ? null
                : randomValueOtherThan(lifecycle, DataStreamLifecycleTests::randomFailuresLifecycle);
            default -> throw new IllegalArgumentException("illegal randomisation branch");
        }
        return new DataStreamFailureStore(enabled, lifecycle);
    }

    @Override
    protected DataStreamFailureStore doParseInstance(XContentParser parser) throws IOException {
        return DataStreamFailureStore.fromXContent(parser);
    }

    static DataStreamFailureStore randomFailureStore() {
        boolean enabledDefined = randomBoolean();
        boolean lifecycleDefined = enabledDefined == false || randomBoolean();
        return new DataStreamFailureStore(
            enabledDefined ? randomBoolean() : null,
            lifecycleDefined ? DataStreamLifecycleTests.randomFailuresLifecycle() : null
        );
    }

    public void testInvalidEmptyConfiguration() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DataStreamFailureStore(null, null));
        assertThat(exception.getMessage(), containsString("at least one non-null configuration value"));
    }
}
