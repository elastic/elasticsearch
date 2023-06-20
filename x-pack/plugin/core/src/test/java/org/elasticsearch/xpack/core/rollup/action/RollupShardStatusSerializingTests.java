/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class RollupShardStatusSerializingTests extends AbstractXContentSerializingTestCase<RollupShardStatus> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(RollupBeforeBulkInfo.class, RollupBeforeBulkInfo.NAME, RollupBeforeBulkInfo::new),
                new NamedWriteableRegistry.Entry(RollupAfterBulkInfo.class, RollupAfterBulkInfo.NAME, RollupAfterBulkInfo::new)
            )
        );
    }

    @Override
    protected RollupShardStatus doParseInstance(XContentParser parser) throws IOException {
        return RollupShardStatus.fromXContent(parser);
    }

    @Override
    protected Reader<RollupShardStatus> instanceReader() {
        return RollupShardStatus::new;
    }

    @Override
    protected RollupShardStatus createTestInstance() {
        return new RollupShardStatus(
            new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(5)),
            randomMillisUpToYear9999(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomLongBetween(500_000, 800_000),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            createTestBeforeBulkInfoInstance(),
            createTestAfterBulkInfoInstance(),
            randomFrom(RollupShardIndexerStatus.values())
        );
    }

    private RollupBeforeBulkInfo createTestBeforeBulkInfoInstance() {
        return new RollupBeforeBulkInfo(
            System.currentTimeMillis(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomIntBetween(1, 10)
        );
    }

    private RollupAfterBulkInfo createTestAfterBulkInfoInstance() {
        int randomRestStatusCode = randomBoolean() ? RestStatus.OK.getStatus()
            : randomBoolean() ? RestStatus.INTERNAL_SERVER_ERROR.getStatus()
            : RestStatus.BAD_REQUEST.getStatus();
        return new RollupAfterBulkInfo(
            System.currentTimeMillis(),
            randomLongBetween(1_000, 5_000),
            randomNonNegativeLong(),
            randomLongBetween(1_000, 5_000),
            randomLongBetween(1_000, 5_000),
            randomBoolean(),
            randomRestStatusCode
        );
    }

    @Override
    protected RollupShardStatus mutateInstance(RollupShardStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
