/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TrainedModelCacheMetadataTests extends AbstractChunkedSerializingTestCase<TrainedModelCacheMetadata> {
    @Override
    protected TrainedModelCacheMetadata doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelCacheMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TrainedModelCacheMetadata> instanceReader() {
        return TrainedModelCacheMetadata::new;
    }

    @Override
    protected TrainedModelCacheMetadata createTestInstance() {
        return new TrainedModelCacheMetadata(randomNonNegativeLong());
    }

    @Override
    protected TrainedModelCacheMetadata mutateInstance(TrainedModelCacheMetadata instance) {
        return new TrainedModelCacheMetadata(randomValueOtherThan(instance.version(), () -> randomNonNegativeLong()));
    }
}
