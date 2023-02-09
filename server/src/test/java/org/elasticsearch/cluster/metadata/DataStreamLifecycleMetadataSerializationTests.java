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

public class DataStreamLifecycleMetadataSerializationTests extends AbstractXContentSerializingTestCase<DataStreamLifecycleMetadata> {

    @Override
    protected Writeable.Reader<DataStreamLifecycleMetadata> instanceReader() {
        return DataStreamLifecycleMetadata::new;
    }

    @Override
    protected DataStreamLifecycleMetadata createTestInstance() {
        if (rarely()) {
            return new DataStreamLifecycleMetadata();
        } else {
            return new DataStreamLifecycleMetadata(new TimeValue(randomMillisUpToYear9999()));
        }
    }

    @Override
    protected DataStreamLifecycleMetadata mutateInstance(DataStreamLifecycleMetadata instance) throws IOException {
        // TODO do this better when we add the rollover config
        return createTestInstance();
    }

    @Override
    protected DataStreamLifecycleMetadata doParseInstance(XContentParser parser) throws IOException {
        return DataStreamLifecycleMetadata.fromXContent(parser);
    }
}
