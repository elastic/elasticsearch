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

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NewCommitNotificationRequestTests extends AbstractWireSerializingTestCase<NewCommitNotificationRequest> {

    private IndexShardRoutingTable indexShardRoutingTable;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexShardRoutingTable = randomIndexShardRoutingTable();
    }

    @Override
    protected NewCommitNotificationRequest createTestInstance() {
        final NewCommitNotificationRequest request = randomRequest();
        assertThat(randomRequest().validate(), nullValue());
        return request;
    }

    @Override
    protected NewCommitNotificationRequest mutateInstance(NewCommitNotificationRequest instance) throws IOException {
        final StatelessCompoundCommit compoundCommit = instance.getCompoundCommit();

        final var i = between(0, 3);
        return switch (i) {
            // Mutate CC's primary and generation
            case 0 -> {
                final var ccTermAndGen = compoundCommit.primaryTermAndGeneration();
                final var newCcTermAndGen = randomValueOtherThan(
                    ccTermAndGen,
                    () -> new PrimaryTermAndGeneration(
                        ccTermAndGen.primaryTerm() + between(-5, 5),
                        ccTermAndGen.generation() + between(-5, 5)
                    )
                );
                yield new NewCommitNotificationRequest(
                    indexShardRoutingTable,
                    new StatelessCompoundCommit(
                        compoundCommit.shardId(),
                        newCcTermAndGen.generation(),
                        newCcTermAndGen.primaryTerm(),
                        compoundCommit.nodeEphemeralId(),
                        compoundCommit.commitFiles(),
                        compoundCommit.sizeInBytes(),
                        compoundCommit.internalFiles()
                    ),
                    newCcTermAndGen.generation(),
                    instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                    instance.getClusterStateVersion(),
                    instance.getNodeId()
                );
            }
            // Mutate latest uploaded BCC's primary and generation
            case 1 -> {
                final var bccGeneration = instance.getBatchedCompoundCommitGeneration();
                final var uploadedBccTermAndGen = instance.getLatestUploadedBatchedCompoundCommitTermAndGen();
                final var newUploadedBccTermAndGen = randomValueOtherThan(
                    uploadedBccTermAndGen,
                    () -> randomFrom(
                        new PrimaryTermAndGeneration(randomLongBetween(1, compoundCommit.primaryTerm() - 1), randomLongBetween(1, 100)),
                        new PrimaryTermAndGeneration(compoundCommit.primaryTerm(), randomLongBetween(1, bccGeneration - 1)),
                        null // for new shards where uploads are yet to happen
                    )
                );
                yield new NewCommitNotificationRequest(
                    indexShardRoutingTable,
                    compoundCommit,
                    bccGeneration,
                    newUploadedBccTermAndGen,
                    instance.getClusterStateVersion(),
                    instance.getNodeId()
                );
            }
            // Mutate cluster state version
            case 2 -> new NewCommitNotificationRequest(
                indexShardRoutingTable,
                compoundCommit,
                instance.getBatchedCompoundCommitGeneration(),
                instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                randomValueOtherThan(instance.getClusterStateVersion(), ESTestCase::randomNonNegativeLong),
                instance.getNodeId()
            );
            // Mutate node id
            case 3 -> new NewCommitNotificationRequest(
                indexShardRoutingTable,
                compoundCommit,
                instance.getBatchedCompoundCommitGeneration(),
                instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                instance.getClusterStateVersion(),
                randomValueOtherThan(instance.getNodeId(), ESTestCase::randomIdentifier)
            );
            default -> throw new IllegalArgumentException("Unexpected value " + i);
        };
    }

    @Override
    protected Writeable.Reader<NewCommitNotificationRequest> instanceReader() {
        return NewCommitNotificationRequest::new;
    }

    public void testValidationErrors() {
        final long primaryTerm = randomLongBetween(1, 42);
        final long generation = randomLongBetween(1, 100);
        final StatelessCompoundCommit compoundCommit = new StatelessCompoundCommit(
            indexShardRoutingTable.shardId(),
            generation,
            primaryTerm,
            randomUUID(),
            Map.of(),
            randomLongBetween(10, 100),
            Set.of()
        );

        final var request1 = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            compoundCommit,
            generation + 1,
            new PrimaryTermAndGeneration(primaryTerm, generation + 2),
            randomNonNegativeLong(),
            randomIdentifier()
        );

        final ActionRequestValidationException validationException1 = request1.validate();
        assertThat(
            validationException1.getMessage(),
            allOf(
                containsString(
                    "compound commit generation [" + generation + "] < batched compound commit generation [" + (generation + 1) + "]"
                ),
                containsString(
                    "batched compound commit generation ["
                        + (generation + 1)
                        + "] < latest uploaded batched compound commit generation ["
                        + (generation + 2)
                        + "]"
                )
            )
        );

        final var request2 = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            compoundCommit,
            generation,
            new PrimaryTermAndGeneration(primaryTerm + 1, generation),
            randomNonNegativeLong(),
            randomIdentifier()
        );

        final ActionRequestValidationException validationException2 = request2.validate();
        assertThat(
            validationException2.getMessage(),
            containsString(
                "batched compound commit primary term ["
                    + (primaryTerm)
                    + "] < latest uploaded batched compound commit primary term ["
                    + (primaryTerm + 1)
                    + "]"
            )
        );
    }

    public void testIsUpload() {
        final long primaryTerm = randomLongBetween(10, 42);
        final long generation = randomLongBetween(10, 100);
        final long bccGeneration = randomLongBetween(5, generation);
        final StatelessCompoundCommit statelessCompoundCommit = new StatelessCompoundCommit(
            indexShardRoutingTable.shardId(),
            generation,
            primaryTerm,
            randomUUID(),
            Map.of(),
            randomLongBetween(10, 100),
            Set.of()
        );
        final long clusterStateVersion = randomNonNegativeLong();
        final String nodeId = randomIdentifier();

        var request = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            statelessCompoundCommit,
            bccGeneration,
            null,
            clusterStateVersion,
            nodeId
        );
        assertThat(request.toString(), request.isUploaded(), is(false));

        request = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            statelessCompoundCommit,
            bccGeneration,
            new PrimaryTermAndGeneration(randomLongBetween(1, primaryTerm), randomLongBetween(1, bccGeneration - 1)),
            clusterStateVersion,
            nodeId
        );
        assertThat(request.toString(), request.isUploaded(), is(false));

        request = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            statelessCompoundCommit,
            bccGeneration,
            new PrimaryTermAndGeneration(primaryTerm, bccGeneration),
            clusterStateVersion,
            nodeId
        );
        assertThat(request.toString(), request.isUploaded(), is(true));
    }

    public void testSerializationNewToOld() throws IOException {
        final var instance = createTestInstance();
        final TransportVersion previousVersion = TransportVersionUtils.getPreviousVersion(
            NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID
        );

        var deserialized = copyInstance(instance, previousVersion);
        try {
            assertThat(deserialized.shardId(), equalTo(instance.shardId()));
            assertThat(deserialized.getBatchedCompoundCommitGeneration(), equalTo(instance.getBatchedCompoundCommitGeneration()));
            assertThat(deserialized.getCompoundCommit(), equalTo(instance.getCompoundCommit()));
            assertThat(
                deserialized.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                equalTo(instance.getLatestUploadedBatchedCompoundCommitTermAndGen())
            );
            assertThat(deserialized.getClusterStateVersion(), nullValue());
            assertThat(deserialized.getNodeId(), nullValue());
        } finally {
            dispose(deserialized);
        }
    }

    public void testDeserializationOldToNew() throws IOException {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(
                TransportVersionUtils.getNextVersion(NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID, true)
            );
            final var parentTaskId = new TaskId(randomIdentifier(), randomNonNegativeLong());
            final var instance = createTestInstance();

            // old logic to serialize NewCommitNotificationRequest without node id
            var broadcast = new BroadcastUnpromotableRequest(indexShardRoutingTable);
            broadcast.setParentTask(parentTaskId);
            broadcast.writeTo(out);
            instance.getCompoundCommit().writeTo(out);
            out.writeVLong(instance.getBatchedCompoundCommitGeneration());
            out.writeOptionalWriteable(instance.getLatestUploadedBatchedCompoundCommitTermAndGen());

            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(
                    TransportVersionUtils.getPreviousVersion(NEW_COMMIT_NOTIFICATION_WITH_CLUSTER_STATE_VERSION_AND_NODE_ID)
                );
                var deserialized = instanceReader().read(in);
                try {
                    assertThat(deserialized.shardId(), equalTo(instance.shardId()));
                    assertThat(deserialized.getBatchedCompoundCommitGeneration(), equalTo(instance.getBatchedCompoundCommitGeneration()));
                    assertThat(deserialized.getCompoundCommit(), equalTo(instance.getCompoundCommit()));
                    assertThat(
                        deserialized.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                        equalTo(instance.getLatestUploadedBatchedCompoundCommitTermAndGen())
                    );
                    assertThat(deserialized.getClusterStateVersion(), nullValue());
                    assertThat(deserialized.getNodeId(), nullValue());
                } finally {
                    dispose(deserialized);
                }
            }
        }
    }

    public static IndexShardRoutingTable randomIndexShardRoutingTable() {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var shardRouting = TestShardRouting.newShardRouting(shardId, null, true, ShardRoutingState.UNASSIGNED);
        final var builder = new IndexShardRoutingTable.Builder(shardId);
        builder.addShard(shardRouting);
        return builder.build();
    }

    private NewCommitNotificationRequest randomRequest() {
        if (randomBoolean()) {
            return randomRequestWithSingleCC();
        }
        final long primaryTerm = randomLongBetween(10, 42);
        final long generation = randomLongBetween(10, 100);
        final long bccGeneration = randomLongBetween(5, generation);

        return new NewCommitNotificationRequest(
            indexShardRoutingTable,
            new StatelessCompoundCommit(
                indexShardRoutingTable.shardId(),
                generation,
                primaryTerm,
                randomUUID(),
                Map.of(),
                randomLongBetween(10, 100),
                Set.of()
            ),
            bccGeneration,
            randomFrom(
                new PrimaryTermAndGeneration(primaryTerm - between(1, 9), randomLongBetween(1, 100)),
                new PrimaryTermAndGeneration(primaryTerm, bccGeneration - between(0, 4)),
                null // for new shards where uploads are yet to happen
            ),
            randomNonNegativeLong(),
            randomIdentifier()
        );
    }

    private NewCommitNotificationRequest randomRequestWithSingleCC() {
        final long primaryTerm = randomLongBetween(10, 42);
        final long generation = randomLongBetween(10, 100);

        return new NewCommitNotificationRequest(
            indexShardRoutingTable,
            new StatelessCompoundCommit(
                indexShardRoutingTable.shardId(),
                generation,
                primaryTerm,
                randomUUID(),
                Map.of(),
                randomLongBetween(10, 100),
                Set.of()
            ),
            generation,
            new PrimaryTermAndGeneration(primaryTerm, generation),
            randomNonNegativeLong(),
            randomIdentifier()
        );
    }
}
