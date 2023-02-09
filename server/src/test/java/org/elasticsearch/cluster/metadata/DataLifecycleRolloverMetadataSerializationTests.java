/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataLifecycleRolloverMetadataSerializationTests extends AbstractXContentSerializingTestCase<DataLifecycleRolloverMetadata> {

    @Override
    protected Writeable.Reader<DataLifecycleRolloverMetadata> instanceReader() {
        return DataLifecycleRolloverMetadata::new;
    }

    @Override
    protected DataLifecycleRolloverMetadata createTestInstance() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new DataLifecycleRolloverMetadata(
                TimeValue.timeValueMillis(randomMillisUpToYear9999()),
                null,
                randomLongBetween(1, 1000)
            );
            case 1 -> new DataLifecycleRolloverMetadata(null, ByteSizeValue.ofGb(randomIntBetween(1, 100)), randomLongBetween(1, 1000));
            default -> new DataLifecycleRolloverMetadata(
                TimeValue.timeValueMillis(randomMillisUpToYear9999()),
                ByteSizeValue.ofGb(randomIntBetween(1, 100)),
                randomLongBetween(1, 1000)
            );
        };
    }

    @Override
    protected DataLifecycleRolloverMetadata mutateInstance(DataLifecycleRolloverMetadata instance) throws IOException {
        var maxAge = instance.getMaxAge();
        var maxPrimaryShardSize = instance.getMaxPrimaryShardSize();
        var minDocs = instance.getMinDocs();
        switch (randomIntBetween(0, 2)) {
            case 0 -> maxAge = TimeValue.timeValueMillis((maxAge == null ? 0 : maxAge.millis()) + randomMillisUpToYear9999());
            case 1 -> maxPrimaryShardSize = ByteSizeValue.ofGb(
                (maxPrimaryShardSize == null ? 0 : maxPrimaryShardSize.getBytes()) + randomIntBetween(1, 100)
            );
            default -> minDocs += randomIntBetween(1, 1000);
        }
        return new DataLifecycleRolloverMetadata(maxAge, maxPrimaryShardSize, minDocs);
    }

    @Override
    protected DataLifecycleRolloverMetadata doParseInstance(XContentParser parser) throws IOException {
        return DataLifecycleRolloverMetadata.fromXContent(parser);
    }
}
