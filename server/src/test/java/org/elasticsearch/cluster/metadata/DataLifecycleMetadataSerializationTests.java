/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataLifecycleMetadataSerializationTests extends AbstractXContentSerializingTestCase<DataLifecycleMetadata> {

    @Override
    protected Writeable.Reader<DataLifecycleMetadata> instanceReader() {
        return DataLifecycleMetadata::new;
    }

    @Override
    protected DataLifecycleMetadata createTestInstance() {
        if (randomBoolean()) {
            return new DataLifecycleMetadata();
        } else {
            return new DataLifecycleMetadata(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
        }
    }

    @Override
    protected DataLifecycleMetadata mutateInstance(DataLifecycleMetadata instance) throws IOException {
        if (instance.getDataRetention() == null) {
            return new DataLifecycleMetadata(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
        }
        return new DataLifecycleMetadata(TimeValue.timeValueMillis(instance.getDataRetention().millis() + randomMillisUpToYear9999()));
    }

    @Override
    protected DataLifecycleMetadata doParseInstance(XContentParser parser) throws IOException {
        return DataLifecycleMetadata.fromXContent(parser);
    }
}
