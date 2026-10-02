/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.OpenPITContextInfo;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.OpenPITReshardingState;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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
            randomSearchContextId(),
            randomMetadata(),
            randomReshardingState()
        );
    }

    private static SearchContextIdForNode randomSearchContextId() {
        return new SearchContextIdForNode(
            randomAlphanumericOfLength(8),
            randomAlphanumericOfLength(8),
            new ShardSearchContextId(randomAlphaOfLength(10), randomLongBetween(0, 1000), randomAlphaOfLength(10))
        );
    }

    private static Map<String, BlobFileRanges> randomMetadata() {
        return randomMetadataWithTs(usually() ? randomTimestampFieldValueRange() : null);
    }

    private static Map<String, BlobFileRanges> randomMetadataWithTs(StatelessCompoundCommit.TimestampFieldValueRange timestampRange) {
        return randomMap(
            1,
            10,
            () -> new Tuple<>(
                randomAlphaOfLength(10),
                createBlobFileRanges(
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    timestampRange
                )
            )
        );
    }

    private static OpenPITReshardingState randomReshardingState() {
        int shards = randomIntBetween(1, 10);
        if (randomBoolean()) {
            return new OpenPITReshardingState(null, SplitShardCountSummary.fromInt(shards));
        }
        return new OpenPITReshardingState(
            IndexReshardingMetadata.newSplitByMultiple(randomIntBetween(1, 10), 2),
            randomBoolean() ? SplitShardCountSummary.fromInt(shards) : SplitShardCountSummary.fromInt(shards * 2)
        );
    }

    @Override
    protected OpenPITContextInfo mutateInstance(OpenPITContextInfo instance) throws IOException {
        int i = randomIntBetween(0, 5);
        return switch (i) {
            case 0 -> new OpenPITContextInfo(
                randomValueOtherThan(instance.shardId(), OpenPITContextInfoSerializationTests::randomShardId),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata(),
                instance.reshardingState()
            );
            case 1 -> new OpenPITContextInfo(
                instance.shardId(),
                randomValueOtherThan(instance.segmentsFileName(), () -> randomAlphaOfLength(10)),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata(),
                instance.reshardingState()

            );
            case 2 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                randomValueOtherThan(instance.keepAlive(), () -> randomLongBetween(0, 1000)),
                instance.contextId(),
                instance.metadata(),
                instance.reshardingState()
            );
            case 3 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                randomValueOtherThan(instance.contextId(), OpenPITContextInfoSerializationTests::randomSearchContextId),
                instance.metadata(),
                instance.reshardingState()
            );
            case 4 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                randomMetadata(),
                instance.reshardingState()
            );
            case 5 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata(),
                randomValueOtherThan(instance.reshardingState(), OpenPITContextInfoSerializationTests::randomReshardingState)
            );
            default -> throw new IllegalStateException("Unexpected value " + i);
        };
    }

    public void testReshardingStateAbsentAtVersionBeforeReshardingMetadataInPitRelocation() throws IOException {
        final var oldVersion = TransportVersionUtils.randomVersionNotSupporting(
            TransportVersion.fromName("resharding_metadata_in_pit_relocation")
        );

        final var original = createTestInstance();
        final var copy = copyInstance(original, oldVersion);
        assertThat(copy.reshardingState(), equalTo(OpenPITReshardingState.notPresent()));
    }

    public void testTimestampDroppedAtVersionBeforeBlobFileRangesInPitRelocation() throws IOException {
        final var oldVersion = TransportVersionUtils.randomVersionNotSupporting(
            TransportVersion.fromName("blob_file_ranges_in_pit_relocation")
        );

        final var original = instanceWithTimestampedMetadata();
        final var copy = copyInstance(original, oldVersion);

        assertThat(copy.metadata().keySet(), equalTo(original.metadata().keySet()));
        for (String file : original.metadata().keySet()) {
            assertThat(copy.metadata().get(file).blobLocation(), equalTo(original.metadata().get(file).blobLocation()));
            assertThat(copy.metadata().get(file).timestampRange(), nullValue()); // old wire format has no timestamp
        }
    }

    public void testReshardingStatePreservedAndTimestampDroppedAtVersionBeforeBlobFileRangesInPitRelocation() throws IOException {
        final var middleVersion = randomVersionSupportingReshardingButNotBlobFileRangesInPitRelocation();

        final var original = instanceWithTimestampedMetadata();
        final var copy = copyInstance(original, middleVersion);

        assertThat(copy.reshardingState(), equalTo(original.reshardingState()));
        assertThat(copy.metadata().keySet(), equalTo(original.metadata().keySet()));
        for (String file : original.metadata().keySet()) {
            assertThat(copy.metadata().get(file).blobLocation(), equalTo(original.metadata().get(file).blobLocation()));
            assertThat(copy.metadata().get(file).timestampRange(), nullValue()); // wire format still BlobLocation-only
        }
    }

    /// New TransportVersion keeps timestamp.
    public void testMetadataPreservesTimestampAtSupportingVersion() throws IOException {
        final var original = instanceWithTimestampedMetadata();
        final var copy = copyInstance(original, TransportVersion.current());
        assertThat(copy.metadata(), equalTo(original.metadata()));
    }

    private static TransportVersion randomVersionSupportingReshardingButNotBlobFileRangesInPitRelocation() {
        final var reshardingVersion = TransportVersion.fromName("resharding_metadata_in_pit_relocation");
        final var blobFileRangesVersion = TransportVersion.fromName("blob_file_ranges_in_pit_relocation");
        return randomFrom(
            TransportVersionUtils.allReleasedVersions()
                .stream()
                .filter(v -> v.supports(reshardingVersion) && v.supports(blobFileRangesVersion) == false)
                .toList()
        );
    }

    private static OpenPITContextInfo instanceWithTimestampedMetadata() {
        final var timestamp = randomTimestampFieldValueRange();
        final var metadata = randomMetadataWithTs(timestamp);
        return new OpenPITContextInfo(
            randomShardId(),
            randomAlphaOfLength(10),
            randomLongBetween(0, 1000),
            randomSearchContextId(),
            metadata,
            randomReshardingState()
        );
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
