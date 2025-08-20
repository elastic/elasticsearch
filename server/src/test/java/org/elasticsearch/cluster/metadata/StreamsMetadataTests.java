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
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class StreamsMetadataTests extends AbstractChunkedSerializingTestCase<StreamsMetadata> {
    @Override
    protected StreamsMetadata doParseInstance(XContentParser parser) throws IOException {
        return StreamsMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<StreamsMetadata> instanceReader() {
        return StreamsMetadata::new;
    }

    @Override
    protected StreamsMetadata createTestInstance() {
        return new StreamsMetadata(randomBoolean());
    }

    @Override
    protected StreamsMetadata mutateInstance(StreamsMetadata instance) throws IOException {
        return new StreamsMetadata(instance.logsEnabled == false);
    }
}
