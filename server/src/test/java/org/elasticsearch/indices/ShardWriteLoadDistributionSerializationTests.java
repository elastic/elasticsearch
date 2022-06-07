/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ShardWriteLoadDistributionSerializationTests extends AbstractSerializingTestCase<ShardWriteLoadDistribution> {

    @Override
    protected ShardWriteLoadDistribution doParseInstance(XContentParser parser) throws IOException {
        return ShardWriteLoadDistribution.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ShardWriteLoadDistribution> instanceReader() {
        return ShardWriteLoadDistribution::new;
    }

    @Override
    protected ShardWriteLoadDistribution createTestInstance() {
        return new ShardWriteLoadDistribution(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)),
            randomBoolean(),
            randomLoadDistribution(),
            randomLoadDistribution(),
            randomLoadDistribution()
        );
    }

    @Override
    protected ShardWriteLoadDistribution mutateInstance(ShardWriteLoadDistribution instance) throws IOException {
        return new ShardWriteLoadDistribution(
            instance.timestamp(),
            instance.dataStream(),
            new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)),
            randomBoolean(),
            randomLoadDistribution(),
            randomLoadDistribution(),
            randomLoadDistribution()
        );
    }

    private LoadDistribution randomLoadDistribution() {
        return new LoadDistribution(
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true)
        );
    }
}
