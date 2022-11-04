/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class IndexWriteLoadSerializationTests extends AbstractXContentSerializingTestCase<IndexWriteLoad> {

    @Override
    protected IndexWriteLoad doParseInstance(XContentParser parser) throws IOException {
        return IndexWriteLoad.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<IndexWriteLoad> instanceReader() {
        return IndexWriteLoad::new;
    }

    @Override
    protected IndexWriteLoad createTestInstance() {
        final int numberOfShards = randomIntBetween(1, 10);
        final var indexWriteLoad = IndexWriteLoad.builder(numberOfShards);
        for (int i = 0; i < numberOfShards; i++) {
            indexWriteLoad.withShardWriteLoad(i, randomDoubleBetween(1, 10, true), randomLongBetween(1, 1000));
        }
        return indexWriteLoad.build();
    }

    @Override
    protected IndexWriteLoad mutateInstance(IndexWriteLoad instance) throws IOException {
        final int newNumberOfShards;
        if (instance.numberOfShards() > 1 && randomBoolean()) {
            newNumberOfShards = randomIntBetween(1, instance.numberOfShards() - 1);
        } else {
            newNumberOfShards = instance.numberOfShards() + randomIntBetween(1, 5);
        }
        final var indexWriteLoad = IndexWriteLoad.builder(newNumberOfShards);
        for (int i = 0; i < newNumberOfShards; i++) {
            boolean existingShard = i < instance.numberOfShards();
            double shardLoad = existingShard && randomBoolean()
                ? instance.getWriteLoadForShard(i).getAsDouble()
                : randomDoubleBetween(0, 128, true);
            long uptimeInMillis = existingShard && randomBoolean()
                ? instance.getUptimeInMillisForShard(i).getAsLong()
                : randomNonNegativeLong();
            indexWriteLoad.withShardWriteLoad(i, shardLoad, uptimeInMillis);
        }
        return indexWriteLoad.build();
    }
}
