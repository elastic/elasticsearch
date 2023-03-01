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

public class DataLifecycleSerializationTests extends AbstractXContentSerializingTestCase<DataLifecycle> {

    @Override
    protected Writeable.Reader<DataLifecycle> instanceReader() {
        return DataLifecycle::new;
    }

    @Override
    protected DataLifecycle createTestInstance() {
        if (randomBoolean()) {
            return new DataLifecycle();
        } else {
            return new DataLifecycle(randomMillisUpToYear9999());
        }
    }

    @Override
    protected DataLifecycle mutateInstance(DataLifecycle instance) throws IOException {
        if (instance.getDataRetention() == null) {
            return new DataLifecycle(randomMillisUpToYear9999());
        }
        return new DataLifecycle(instance.getDataRetention().millis() + randomMillisUpToYear9999());
    }

    @Override
    protected DataLifecycle doParseInstance(XContentParser parser) throws IOException {
        return DataLifecycle.fromXContent(parser);
    }
}
