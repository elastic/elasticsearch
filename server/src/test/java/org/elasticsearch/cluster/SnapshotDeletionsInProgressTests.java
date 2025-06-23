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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotDeletionsInProgressTests extends SimpleDiffableWireSerializationTestCase<ClusterState.Custom> {

    private Supplier<ProjectId> projectIdSupplier;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        projectIdSupplier = ESTestCase::randomProjectIdOrDefault;
    }

    @Override
    protected ClusterState.Custom createTestInstance() {
        return SnapshotDeletionsInProgress.of(randomList(0, 3, this::randomDeletionEntry));
    }

    @Override
    protected Writeable.Reader<ClusterState.Custom> instanceReader() {
        return SnapshotDeletionsInProgress::new;
    }

    @Override
    protected Writeable.Reader<Diff<ClusterState.Custom>> diffReader() {
        return SnapshotDeletionsInProgress::readDiffFrom;
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
        SnapshotDeletionsInProgress original = (SnapshotDeletionsInProgress) testInstance;
        List<SnapshotDeletionsInProgress.Entry> entries = original.getEntries();
        if (entries.isEmpty()) {
            return SnapshotDeletionsInProgress.of(randomList(1, 3, this::randomDeletionEntry));
        }

        final SnapshotDeletionsInProgress.Entry entry = randomFrom(entries);
        final int testVariant = between(0, 3);
        switch (testVariant) {
            case 0: // Add a new entry
                return original.withAddedEntry(randomDeletionEntry());
            case 1: // Remove an existing entry
                return original.withRemovedEntry(entry.uuid());
            case 2: // With new repo generation or started
                if (entry.state() != SnapshotDeletionsInProgress.State.STARTED) {
                    return original.withRemovedEntry(entry.uuid()).withAddedEntry(entry.started());
                } else {
                    return original.withRemovedEntry(entry.uuid())
                        .withAddedEntry(entry.withRepoGen(entry.repositoryStateId() + randomLongBetween(1, 1000)));
                }
            case 3: // with new or removed snapshot
                final SnapshotDeletionsInProgress.Entry updatedEntry;
                if (entry.snapshots().isEmpty()) {
                    if (entry.state() != SnapshotDeletionsInProgress.State.STARTED) {
                        updatedEntry = entry.withAddedSnapshots(randomList(1, 3, () -> new SnapshotId(randomUUID(), randomUUID())));
                    } else {
                        updatedEntry = entry.withSnapshots(randomList(1, 3, () -> new SnapshotId(randomUUID(), randomUUID())));
                    }
                } else {
                    updatedEntry = entry.withSnapshots(randomSubsetOf(between(0, entry.snapshots().size() - 1), entry.snapshots()));
                }
                return original.withRemovedEntry(entry.uuid()).withAddedEntry(updatedEntry);
            default:
                throw new AssertionError("Unexpected variant: " + testVariant);
        }
    }

    private SnapshotDeletionsInProgress.Entry randomDeletionEntry() {
        final List<SnapshotId> snapshots = randomList(0, 3, () -> new SnapshotId(randomUUID(), randomUUID()));
        return new SnapshotDeletionsInProgress.Entry(
            projectIdSupplier.get(),
            randomIdentifier(),
            snapshots,
            randomLongBetween(0, 9999),
            randomLongBetween(0, 9999),
            snapshots.isEmpty() ? SnapshotDeletionsInProgress.State.WAITING : randomFrom(SnapshotDeletionsInProgress.State.values())
        );
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
        final SnapshotDeletionsInProgress fromStream = new SnapshotDeletionsInProgress(in);
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
        final NamedDiff<ClusterState.Custom> diffFromStream = SnapshotDeletionsInProgress.readDiffFrom(in);

        assertThat(diffFromStream.apply(before), equalTo(after));
    }

    public void testXContent() throws IOException {
        final var projectId = randomProjectIdOrDefault();
        SnapshotDeletionsInProgress sdip = SnapshotDeletionsInProgress.of(
            List.of(
                new SnapshotDeletionsInProgress.Entry(
                    projectId,
                    "repo",
                    Collections.emptyList(),
                    736694267638L,
                    0,
                    SnapshotDeletionsInProgress.State.STARTED
                )
            )
        );

        try (XContentBuilder builder = jsonBuilder()) {
            builder.humanReadable(true);
            builder.startObject();
            ChunkedToXContent.wrapAsToXContent(sdip).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = Strings.toString(builder);
            assertThat(json, equalTo(XContentHelper.stripWhitespace(Strings.format("""
                {
                  "snapshot_deletions": [
                    {
                      "project_id": "%s",
                      "repository": "repo",
                      "snapshots": [],
                      "start_time": "1993-05-06T13:17:47.638Z",
                      "start_time_millis": 736694267638,
                      "repository_state_id": 0,
                      "state": "STARTED"
                    }
                  ]
                }""", projectId.id()))));
        }
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            SnapshotDeletionsInProgress.of(
                randomList(
                    10,
                    () -> new SnapshotDeletionsInProgress.Entry(
                        randomProjectIdOrDefault(),
                        randomAlphaOfLength(10),
                        Collections.emptyList(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomFrom(SnapshotDeletionsInProgress.State.values())
                    )
                )
            ),
            instance -> instance.getEntries().size() + 2
        );
    }
}
