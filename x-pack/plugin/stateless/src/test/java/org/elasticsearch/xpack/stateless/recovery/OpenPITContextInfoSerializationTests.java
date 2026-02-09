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

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.OpenPITContextInfo;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;

public class OpenPITContextInfoSerializationTests extends AbstractWireSerializingTestCase<OpenPITContextInfo> {

    @Override
    protected Writeable.Reader<OpenPITContextInfo> instanceReader() {
        return OpenPITContextInfo::new;
    }

    @Override
    protected OpenPITContextInfo createTestInstance() {
        return new OpenPITContextInfo(
            randomShardId(),
            randomAlphaOfLength(10),
            randomLongBetween(0, 1000),
            createSearchContextId(),
            createRandomMetadata()
        );
    }

    SearchContextIdForNode createSearchContextId() {
        return new SearchContextIdForNode(
            randomAlphanumericOfLength(8),
            randomAlphanumericOfLength(8),
            new ShardSearchContextId(randomAlphaOfLength(10), randomLongBetween(0, 1000), randomAlphaOfLength(10))
        );
    }

    Map<String, BlobLocation> createRandomMetadata() {
        return randomMap(
            1,
            10,
            () -> new Tuple<>(
                randomAlphaOfLength(10),
                createBlobLocation(
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000)
                )
            )
        );
    }

    @Override
    protected OpenPITContextInfo mutateInstance(OpenPITContextInfo instance) throws IOException {
        int i = randomIntBetween(0, 4);
        return switch (i) {
            case 0 -> new OpenPITContextInfo(
                randomValueOtherThan(instance.shardId(), OpenPITContextInfoSerializationTests::randomShardId),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata()
            );
            case 1 -> new OpenPITContextInfo(
                instance.shardId(),
                randomValueOtherThan(instance.segmentsFileName(), () -> randomAlphaOfLength(10)),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata()

            );
            case 2 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                randomValueOtherThan(instance.keepAlive(), () -> randomLongBetween(0, 1000)),
                instance.contextId(),
                instance.metadata()
            );
            case 3 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                randomValueOtherThan(instance.contextId(), this::createSearchContextId),
                instance.metadata()
            );
            case 4 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                createRandomMetadata()
            );
            default -> throw new IllegalStateException("Unexpected value " + i);
        };
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
