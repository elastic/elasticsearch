/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeTests;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

public class PublishShardSizesRequestTests extends AbstractWireSerializingTestCase<PublishShardSizesRequest> {

    @Override
    protected Writeable.Reader<PublishShardSizesRequest> instanceReader() {
        return PublishShardSizesRequest::new;
    }

    @Override
    protected PublishShardSizesRequest createTestInstance() {
        return new PublishShardSizesRequest(UUIDs.randomBase64UUID(), randomShardSizes());
    }

    private static ShardId randomShardId() {
        return new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(), randomIntBetween(0, 99));
    }

    private static Map<ShardId, ShardSizeStatsReader.ShardSize> randomShardSizes() {
        return randomMap(1, 25, () -> Tuple.tuple(randomShardId(), ShardSizeTests.randomShardSize()));
    }

    @Override
    protected PublishShardSizesRequest mutateInstance(PublishShardSizesRequest instance) {
        return switch (randomInt(1)) {
            case 0 -> new PublishShardSizesRequest(
                randomValueOtherThan(instance.getNodeId(), UUIDs::randomBase64UUID),
                instance.getShardSizes()
            );
            default -> new PublishShardSizesRequest(
                instance.getNodeId(),
                randomValueOtherThan(instance.getShardSizes(), PublishShardSizesRequestTests::randomShardSizes)
            );
        };
    }
}
