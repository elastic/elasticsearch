/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class RepositoryCleanupInProgressTests extends SimpleDiffableWireSerializationTestCase<ClusterState.Custom> {

    private static final TransportVersion PROJECT_ID_IN_SNAPSHOTS_DELETIONS_AND_REPO_CLEANUP = TransportVersion.fromName(
        "project_id_in_snapshots_deletions_and_repo_cleanup"
    );

    private static final TransportVersion REPOSITORY_CLEANUP_ENTRY_UUID = TransportVersion.fromName("repository_cleanup_entry_uuid");

    private Supplier<ProjectId> projectIdSupplier;
    private Supplier<String> uuidSupplier;

    @Before
    public void initSuppliers() throws Exception {
        projectIdSupplier = ESTestCase::randomProjectIdOrDefault;
        uuidSupplier = ESTestCase::randomUUID;
    }

    @Override
    protected ClusterState.Custom createTestInstance() {
        return new RepositoryCleanupInProgress(randomList(0, 3, this::randomCleanupEntry));
    }

    @Override
    protected Writeable.Reader<ClusterState.Custom> instanceReader() {
        return RepositoryCleanupInProgress::new;
    }

    @Override
    protected Writeable.Reader<Diff<ClusterState.Custom>> diffReader() {
        return RepositoryCleanupInProgress::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected ClusterState.Custom mutateInstance(ClusterState.Custom instance) throws IOException {
        return makeTestChanges(instance);
    }

    @Override
    protected ClusterState.Custom makeTestChanges(ClusterState.Custom testInstance) {
        RepositoryCleanupInProgress original = (RepositoryCleanupInProgress) testInstance;
        final List<RepositoryCleanupInProgress.Entry> entries = original.entries();

        if (entries.isEmpty()) {
            return new RepositoryCleanupInProgress(randomList(1, 3, this::randomCleanupEntry));
        }

        final int testVariant = between(0, 1);

        switch (testVariant) {
            case 0: // Add a new entry
                return new RepositoryCleanupInProgress(CollectionUtils.appendToCopy(entries, randomCleanupEntry()));
            case 1: // Remove an existing entry
                return new RepositoryCleanupInProgress(randomSubsetOf(between(0, entries.size() - 1), entries));
            default:
                throw new AssertionError("Unexpected variant: " + testVariant);
        }
    }

    private RepositoryCleanupInProgress.Entry randomCleanupEntry() {
        return new RepositoryCleanupInProgress.Entry(
            projectIdSupplier.get(),
            randomIdentifier(),
            randomLongBetween(0, 9999),
            uuidSupplier.get()
        );
    }

    public void testSerializationBwc() throws IOException {
        projectIdSupplier = () -> ProjectId.DEFAULT;
        uuidSupplier = () -> RepositoryCleanupInProgress.Entry.UNKNOWN_UUID;
        final var oldVersion = TransportVersionUtils.getPreviousVersion(PROJECT_ID_IN_SNAPSHOTS_DELETIONS_AND_REPO_CLEANUP);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        final ClusterState.Custom original = createTestInstance();
        original.writeTo(out);

        final var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        final RepositoryCleanupInProgress fromStream = new RepositoryCleanupInProgress(in);
        assertThat(fromStream, equalTo(original));
    }

    public void testDiffSerializationBwc() throws IOException {
        projectIdSupplier = () -> ProjectId.DEFAULT;
        uuidSupplier = () -> RepositoryCleanupInProgress.Entry.UNKNOWN_UUID;
        final var oldVersion = TransportVersionUtils.getPreviousVersion(PROJECT_ID_IN_SNAPSHOTS_DELETIONS_AND_REPO_CLEANUP);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);

        final ClusterState.Custom before = createTestInstance();
        final ClusterState.Custom after = makeTestChanges(before);
        final Diff<ClusterState.Custom> diff = after.diff(before);
        diff.writeTo(out);

        final var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), getNamedWriteableRegistry());
        in.setTransportVersion(oldVersion);
        final NamedDiff<ClusterState.Custom> diffFromStream = RepositoryCleanupInProgress.readDiffFrom(in);

        assertThat(diffFromStream.apply(before), equalTo(after));
    }

    /** A node which predates the cleanup UUID neither sends nor expects one, so the entry reads back with the unknown UUID. */
    public void testCleanupUuidSerializationBwc() throws IOException {
        final var oldVersion = TransportVersionUtils.getPreviousVersion(REPOSITORY_CLEANUP_ENTRY_UUID);
        final var original = new RepositoryCleanupInProgress.Entry(
            ProjectId.DEFAULT,
            randomIdentifier(),
            randomNonNegativeLong(),
            randomUUID()
        );
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        original.writeTo(out);

        final var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        assertThat(
            RepositoryCleanupInProgress.Entry.readFrom(in),
            equalTo(
                new RepositoryCleanupInProgress.Entry(
                    original.projectId(),
                    original.repository(),
                    original.repositoryStateId(),
                    RepositoryCleanupInProgress.Entry.UNKNOWN_UUID
                )
            )
        );
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new RepositoryCleanupInProgress(
                randomList(
                    10,
                    () -> new RepositoryCleanupInProgress.Entry(
                        randomProjectIdOrDefault(),
                        randomAlphaOfLength(10),
                        randomNonNegativeLong()
                    )
                )
            ),
            i -> i.entries().size() + 2
        );
    }
}
