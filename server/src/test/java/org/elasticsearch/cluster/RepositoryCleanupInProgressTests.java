/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersions;
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

    private Supplier<ProjectId> projectIdSupplier;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        projectIdSupplier = ESTestCase::randomProjectIdOrDefault;
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
        return RepositoryCleanupInProgress.startedEntry(projectIdSupplier.get(), randomIdentifier(), randomLongBetween(0, 9999));
    }

    public void testSerializationBwc() throws IOException {
        projectIdSupplier = () -> ProjectId.DEFAULT;
        final var oldVersion = TransportVersionUtils.getPreviousVersion(
            TransportVersions.PROJECT_ID_IN_SNAPSHOTS_DELETIONS_AND_REPO_CLEANUP
        );
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
        final var oldVersion = TransportVersionUtils.getPreviousVersion(
            TransportVersions.PROJECT_ID_IN_SNAPSHOTS_DELETIONS_AND_REPO_CLEANUP
        );
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
