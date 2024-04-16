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
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.NEW_COMMIT_NOTIFICATION_WITH_BCC_INFO;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
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

        final var i = between(0, 1);
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
                    instance.getLatestUploadedBatchedCompoundCommitTermAndGen()
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
                yield new NewCommitNotificationRequest(indexShardRoutingTable, compoundCommit, bccGeneration, newUploadedBccTermAndGen);
            }
            default -> throw new IllegalArgumentException("Unexpected value " + i);
        };
    }

    @Override
    protected Writeable.Reader<NewCommitNotificationRequest> instanceReader() {
        return NewCommitNotificationRequest::new;
    }

    public void testSerializationBwc() throws IOException {
        final TransportVersion previousVersion = TransportVersionUtils.getPreviousVersion(NEW_COMMIT_NOTIFICATION_WITH_BCC_INFO);
        // BWC only works for singleton BCC
        final NewCommitNotificationRequest request = randomRequestWithSingleCC();
        assertSerialization(request, previousVersion);
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
            new PrimaryTermAndGeneration(primaryTerm, generation + 2)
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
            new PrimaryTermAndGeneration(primaryTerm + 1, generation)
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

        var request = new NewCommitNotificationRequest(indexShardRoutingTable, statelessCompoundCommit, bccGeneration, null);
        assertThat(request.toString(), request.isUpload(), is(false));

        request = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            statelessCompoundCommit,
            bccGeneration,
            new PrimaryTermAndGeneration(randomLongBetween(1, primaryTerm), randomLongBetween(1, bccGeneration - 1))
        );
        assertThat(request.toString(), request.isUpload(), is(false));

        request = new NewCommitNotificationRequest(
            indexShardRoutingTable,
            statelessCompoundCommit,
            bccGeneration,
            new PrimaryTermAndGeneration(primaryTerm, bccGeneration)
        );
        assertThat(request.toString(), request.isUpload(), is(true));
    }

    private IndexShardRoutingTable randomIndexShardRoutingTable() {
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
            )
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
            new PrimaryTermAndGeneration(primaryTerm, generation)
        );
    }
}
