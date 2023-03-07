/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class IndexMetadataStatsSerializationTests extends AbstractXContentSerializingTestCase<IndexMetadataStats> {

    @Override
    protected IndexMetadataStats doParseInstance(XContentParser parser) throws IOException {
        return IndexMetadataStats.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<IndexMetadataStats> instanceReader() {
        return IndexMetadataStats::new;
    }

    @Override
    protected IndexMetadataStats createTestInstance() {
        final int numberOfShards = randomIntBetween(1, 10);
        final var indexWriteLoad = IndexWriteLoad.builder(numberOfShards);
        for (int i = 0; i < numberOfShards; i++) {
            indexWriteLoad.withShardWriteLoad(i, randomDoubleBetween(1, 10, true), randomLongBetween(1, 1000));
        }
        return new IndexMetadataStats(indexWriteLoad.build(), randomLongBetween(1024, 10240), randomIntBetween(1, 4));
    }

    @Override
    protected IndexMetadataStats mutateInstance(IndexMetadataStats originalStats) {
        final IndexWriteLoad originalWriteLoad = originalStats.writeLoad();

        final int newNumberOfShards;
        if (originalWriteLoad.numberOfShards() > 1 && randomBoolean()) {
            newNumberOfShards = randomIntBetween(1, originalWriteLoad.numberOfShards() - 1);
        } else {
            newNumberOfShards = originalWriteLoad.numberOfShards() + randomIntBetween(1, 5);
        }
        final var indexWriteLoad = IndexWriteLoad.builder(newNumberOfShards);
        for (int i = 0; i < newNumberOfShards; i++) {
            boolean existingShard = i < originalWriteLoad.numberOfShards();
            double shardLoad = existingShard && randomBoolean()
                ? originalWriteLoad.getWriteLoadForShard(i).getAsDouble()
                : randomDoubleBetween(0, 128, true);
            long uptimeInMillis = existingShard && randomBoolean()
                ? originalWriteLoad.getUptimeInMillisForShard(i).getAsLong()
                : randomNonNegativeLong();
            indexWriteLoad.withShardWriteLoad(i, shardLoad, uptimeInMillis);
        }
        return new IndexMetadataStats(indexWriteLoad.build(), randomLongBetween(1024, 10240), randomIntBetween(1, 4));
    }
}
