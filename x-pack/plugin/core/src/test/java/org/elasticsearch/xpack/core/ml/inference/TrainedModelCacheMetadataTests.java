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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelCacheMetadata.TrainedModelCacheMetadataEntry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrainedModelCacheMetadataTests extends AbstractChunkedSerializingTestCase<TrainedModelCacheMetadata> {
    public static TrainedModelCacheMetadataEntry randomEntry() {
        return new TrainedModelCacheMetadataEntry(randomIdentifier());
    }

    public static TrainedModelCacheMetadata randomInstance() {
        Map<String, TrainedModelCacheMetadataEntry> entries = Stream.generate(TrainedModelCacheMetadataTests::randomEntry)
            .limit(randomInt(5))
            .collect(Collectors.toMap(TrainedModelCacheMetadataEntry::getModelId, Function.identity(), (k, k1) -> k, HashMap::new));

        return new TrainedModelCacheMetadata(entries);
    }

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
        return randomInstance();
    }

    @Override
    protected TrainedModelCacheMetadata mutateInstance(TrainedModelCacheMetadata instance) {
        return randomValueOtherThan(instance, () -> randomInstance());
    }
}
