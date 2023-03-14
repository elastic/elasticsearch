/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.index.shard.ShardId;
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
        RollupShardStatus rollupShardStatus = new RollupShardStatus(
            new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(5)),
            randomMillisUpToYear9999(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        return rollupShardStatus;
    }

    @Override
    protected RollupShardStatus mutateInstance(RollupShardStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
