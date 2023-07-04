/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RollupShardStatusSerializingTests extends AbstractXContentSerializingTestCase<RollupShardStatus> {

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
        long docsProcessed = randomLongBetween(500_000, 800_000);
        long indexEndTimeMillis = System.currentTimeMillis() + randomLongBetween(400_000, 500_000);
        long indexStartTimeMillis = System.currentTimeMillis() - randomLongBetween(400_000, 500_000);
        long lastIndexingTimestamp = System.currentTimeMillis() + randomLongBetween(200_000, 300_000);
        long lastTargetTimestamp = System.currentTimeMillis() - randomLongBetween(200_000, 300_000);
        long lastSourceTimestamp = System.currentTimeMillis();
        long totalShardDocCount = randomLongBetween(500_000, 800_000);
        long numFailed = randomNonNegativeLong();
        long numIndexed = randomNonNegativeLong();
        long numSent = randomNonNegativeLong();
        long numReceived = randomNonNegativeLong();
        long rollupStart = randomMillisUpToYear9999();
        final ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(5));
        final RollupShardIndexerStatus rollupShardIndexerStatus = randomFrom(RollupShardIndexerStatus.values());
        return new RollupShardStatus(
            shardId,
            rollupStart,
            numReceived,
            numSent,
            numIndexed,
            numFailed,
            totalShardDocCount,
            lastSourceTimestamp,
            lastTargetTimestamp,
            lastIndexingTimestamp,
            indexStartTimeMillis,
            indexEndTimeMillis,
            docsProcessed,
            100.0F * docsProcessed / totalShardDocCount,
            createTestRollupBulkInfo(),
            createTestBeforeBulkInfoInstance(),
            createTestAfterBulkInfoInstance(),
            rollupShardIndexerStatus
        );
    }

    private RollupBulkInfo createTestRollupBulkInfo() {
        return new RollupBulkInfo(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
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
