/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MetadataRepositoriesMetadataTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedWriteableRegistry namedWriteableRegistryBwc;

    @Before
    public void initializeRegistries() {
        namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        namedWriteableRegistryBwc = new NamedWriteableRegistry(
            Stream.concat(
                ClusterModule.getNamedWriteables().stream().filter(entry -> entry.name.equals(RepositoriesMetadata.TYPE) == false),
                Stream.of(
                    new NamedWriteableRegistry.Entry(
                        Metadata.ClusterCustom.class,
                        RepositoriesMetadata.TYPE,
                        TestBwcRepositoryMetadata::new
                    ),
                    new NamedWriteableRegistry.Entry(NamedDiff.class, RepositoriesMetadata.TYPE, TestBwcRepositoryMetadata::readDiffFrom)
                )
            ).toList()
        );
    }

    public void testRepositoriesMetadataSerialization() throws IOException {
        final Metadata orig = randomMetadata(between(0, 5));

        final BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        final Metadata fromStream = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );

        assertTrue(Metadata.isGlobalStateEquals(orig, fromStream));
    }

    public void testRepositoriesMetadataDiffSerialization() throws IOException {
        final Tuple<Metadata, Metadata> tuple = randomMetadataAndUpdate(between(0, 5));
        final Metadata before = tuple.v1();
        final Metadata after = tuple.v2();

        final Diff<Metadata> diff = after.diff(before);

        final BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);

        final Diff<Metadata> diffFromStream = Metadata.readDiffFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        final Metadata metadataFromDiff = diffFromStream.apply(before);

        assertTrue(Metadata.isGlobalStateEquals(after, metadataFromDiff));
    }

    public void testRepositoriesMetadataSerializationBwc() throws IOException {
        {
            final var oldVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersions.MULTI_PROJECT,
                TransportVersionUtils.getPreviousVersion(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)
            );
            final Metadata orig = randomMetadata(between(0, 5), -1);
            doTestRepositoriesMetadataSerializationBwc(orig, oldVersion);
        }

        {
            final var oldVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
            // Before multi-project, BWC is possible for a single project
            final Metadata orig = randomMetadata(0, -1);
            doTestRepositoriesMetadataSerializationBwc(orig, oldVersion);
        }
    }

    private void doTestRepositoriesMetadataSerializationBwc(Metadata orig, TransportVersion oldVersion) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        orig.writeTo(out);

        // Round-trip from new-node writes to old-stream and new-node reads from old-stream
        final var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry);
        in.setTransportVersion(oldVersion);
        final Metadata fromStream = Metadata.readFrom(in);
        assertTrue(Metadata.isGlobalStateEquals(orig, fromStream));

        // Simulate new-node writes to old-stream and old-node reads from old-stream
        simulateReadOnOldNodeAndAssert(out.bytes(), oldVersion, orig);
    }

    private void simulateReadOnOldNodeAndAssert(BytesReference bytesReference, TransportVersion oldVersion, Metadata orig)
        throws IOException {
        final var in = new NamedWriteableAwareStreamInput(bytesReference.streamInput(), namedWriteableRegistryBwc);
        in.setTransportVersion(oldVersion);
        final Metadata fromStream = Metadata.readFrom(in);
        assertMetadataBwcEquals(fromStream, orig);
    }

    public void testRepositoriesMetadataDiffSerializationBwc() throws IOException {
        {
            final var oldVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersions.MULTI_PROJECT,
                TransportVersionUtils.getPreviousVersion(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)
            );
            final Tuple<Metadata, Metadata> tuple = randomMetadataAndUpdate(between(0, 5), -1);
            doTestRepositoriesMetadataDiffSerializationBwc(tuple, oldVersion);
        }

        {
            final var oldVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
            // Before multi-project, BWC is possible for a single project
            final Tuple<Metadata, Metadata> tuple = randomMetadataAndUpdate(0, -1);
            doTestRepositoriesMetadataDiffSerializationBwc(tuple, oldVersion);
        }
    }

    private void doTestRepositoriesMetadataDiffSerializationBwc(Tuple<Metadata, Metadata> tuple, TransportVersion oldVersion)
        throws IOException {
        final Metadata before = tuple.v1();
        final Metadata after = tuple.v2();
        final Diff<Metadata> diff = after.diff(before);

        final BytesStreamOutput out = new BytesStreamOutput();

        out.setTransportVersion(oldVersion);
        diff.writeTo(out);

        // Round-trip from new-node writes diff to old-stream and new-node reads from old-stream
        final var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry);
        in.setTransportVersion(oldVersion);
        final Diff<Metadata> diffFromStream = Metadata.readDiffFrom(in);
        final Metadata metadataFromDiff = diffFromStream.apply(before);
        assertTrue(Metadata.isGlobalStateEquals(after, metadataFromDiff));

        // Simulate new-node writes diff to old-stream and old-node reads from old-stream
        simulateReadAndApplyDiffOnOldNodeAndAssert(out.bytes(), oldVersion, before, after);
    }

    // Simulate the deserialization and application of the diff on an old node
    private void simulateReadAndApplyDiffOnOldNodeAndAssert(
        BytesReference bytesReference,
        TransportVersion oldVersion,
        Metadata before,
        Metadata after
    ) throws IOException {
        final var in = new NamedWriteableAwareStreamInput(bytesReference.streamInput(), namedWriteableRegistryBwc);
        in.setTransportVersion(oldVersion);
        final Diff<Metadata> diffFromStream = Metadata.readDiffFrom(in);

        // On the old node, the "before" metadata would have repositories in the Metadata#customs. We simulate
        // it by move the repositories from the default project into Metadata#customs as a TestBwcRepositoryMetadata
        final Metadata beforeBwc = Metadata.builder(before)
            .putCustom(
                RepositoriesMetadata.TYPE,
                new TestBwcRepositoryMetadata(RepositoriesMetadata.get(before.getProject(ProjectId.DEFAULT)).repositories())
            )
            .put(ProjectMetadata.builder(before.getProject(ProjectId.DEFAULT)).removeCustom(RepositoriesMetadata.TYPE))
            .build();
        // Apply the diff to the BWC "before" metadata and get the "after" metadata on an old node
        final Metadata metadataFromDiff = diffFromStream.apply(beforeBwc);

        assertMetadataBwcEquals(metadataFromDiff, after);
    }

    /**
     * Check equality of the two metadata by handling the different types of RepositoriesMetadata
     * @param metadataBwc The old style metadata with RepositoriesMetadata as Metadata#ClusterCustom
     * @param metadataNew The new style metadata with RepositoriesMetadata as Metadata#ProjectCustom
     */
    private void assertMetadataBwcEquals(Metadata metadataBwc, Metadata metadataNew) {
        // BWC metadata has no repositories in project
        assertThat(metadataBwc.getProject(ProjectId.DEFAULT).custom(RepositoriesMetadata.TYPE), nullValue());
        // New metadata has no repositories in Metadata#customs
        assertThat(metadataNew.custom(RepositoriesMetadata.TYPE), nullValue());

        final Metadata.ClusterCustom custom = metadataBwc.custom(RepositoriesMetadata.TYPE);

        if (custom != null) {
            assertThat(custom, notNullValue());
            assertThat(custom, instanceOf(TestBwcRepositoryMetadata.class));
            assertThat(
                ((TestBwcRepositoryMetadata) custom).repositories,
                equalTo(RepositoriesMetadata.get(metadataNew.getProject(ProjectId.DEFAULT)).repositories())
            );
        } else {
            // If there is no repositories in the deserialized metadataBwc, there should no repositories in the original metadataNew
            assertThat(metadataNew.getProject(ProjectId.DEFAULT).custom(RepositoriesMetadata.TYPE), nullValue());
        }

        // All other parts excluding repositories of the two metadata are equal
        assertTrue(
            Metadata.isGlobalStateEquals(
                Metadata.builder(metadataNew)
                    .put(ProjectMetadata.builder(metadataNew.getProject(ProjectId.DEFAULT)).removeCustom(RepositoriesMetadata.TYPE))
                    .build(),
                Metadata.builder(metadataBwc).removeCustom(RepositoriesMetadata.TYPE).build()
            )
        );
    }

    private static Metadata randomMetadata(int extraProjects) {
        return randomMetadata(extraProjects, between(0, 5));
    }

    /**
     * Randomly create a Metadata object with the default project and extra projects as specified. The default project
     * has random RepositoriesMetadata which can also be null. Each extra project also has a random RepositoriesMetadata
     * containing the specified number of RepositoryMetadata. If the specified number is -1, extra projects will not have
     * any RepositoriesMetadata.
     * @param extraProjects Number of extra projects to create
     * @param numReposPerExtraProject Number of RepositoryMetadata for each extra project. Or -1 to ensure no
     *                                RepositoriesMetadata for extra projects.
     */
    private static Metadata randomMetadata(int extraProjects, int numReposPerExtraProject) {
        final Metadata.Builder builder = Metadata.builder().put(randomProject(ProjectId.DEFAULT, between(0, 5)));
        IntStream.range(0, extraProjects).forEach(i -> builder.put(randomProject(randomUniqueProjectId(), numReposPerExtraProject)));
        return builder.build();
    }

    private static Tuple<Metadata, Metadata> randomMetadataAndUpdate(int extraProjects) {
        return randomMetadataAndUpdate(extraProjects, between(0, 5));
    }

    /**
     * Randomly generate a metadata then randomly mutates its RepositoriesMetadata in all projects.
     * @param extraProjects Number of extra projects
     * @param numReposPerExtraProject Number of RepositoryMetadata the RepositoriesMetadata contains per project or -1 for null.
     */
    private static Tuple<Metadata, Metadata> randomMetadataAndUpdate(int extraProjects, int numReposPerExtraProject) {
        final Metadata before = randomMetadata(extraProjects, numReposPerExtraProject);

        final Metadata.Builder builder = Metadata.builder(before);

        builder.forEachProject(b -> {
            final RepositoriesMetadata repositoriesMetadata = b.getCustom(RepositoriesMetadata.TYPE);
            if (ProjectId.DEFAULT.equals(b.getId()) || numReposPerExtraProject > 0) {
                final RepositoriesMetadata mutatedRepositoriesMetadata = mutateRepositoriesMetadata(repositoriesMetadata);
                if (mutatedRepositoriesMetadata != null) {
                    b.putCustom(RepositoriesMetadata.TYPE, mutatedRepositoriesMetadata);
                } else {
                    b.removeCustom(RepositoriesMetadata.TYPE);
                }
            }
            return b;
        });

        return new Tuple<>(before, builder.build());
    }

    /**
     * Randomly create a ProjectMetadata object with the given ProjectId and RepositoriesMetadata containing specified
     * number of RepositoryMetadata. If the number is -1, no RepositoriesMetadata will be created.
     */
    private static ProjectMetadata randomProject(ProjectId projectId, int numRepos) {
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        if (numRepos < 0) {
            return builder.build();
        }
        final RepositoriesMetadata repositoriesMetadata = randomRepositoriesMetadata();
        if (repositoriesMetadata == null) {
            return builder.build();
        } else {
            return builder.putCustom(RepositoriesMetadata.TYPE, repositoriesMetadata).build();
        }
    }

    @Nullable
    private static RepositoriesMetadata randomRepositoriesMetadata() {
        return randomRepositoriesMetadata(between(0, 5));
    }

    private static RepositoriesMetadata randomRepositoriesMetadata(int numRepos) {
        if (numRepos == 0) {
            return randomBoolean() ? null : RepositoriesMetadata.EMPTY;
        }
        return new RepositoriesMetadata(randomList(numRepos, numRepos, MetadataRepositoriesMetadataTests::randomRepositoryMetadata));
    }

    private static RepositoryMetadata randomRepositoryMetadata() {
        return new RepositoryMetadata(randomIdentifier(), randomUUID(), randomAlphaOfLengthBetween(3, 8), Settings.EMPTY);
    }

    private static RepositoriesMetadata mutateRepositoriesMetadata(@Nullable RepositoriesMetadata repositoriesMetadata) {
        if ((repositoriesMetadata == null || repositoriesMetadata.repositories().isEmpty()) && randomBoolean()) {
            return randomRepositoriesMetadata();
        }

        if (randomBoolean()) {
            return randomRepositoriesMetadata();
        }

        if (repositoriesMetadata == null || repositoriesMetadata.repositories().isEmpty()) {
            return randomRepositoriesMetadata(between(1, 5));
        }

        return new RepositoriesMetadata(repositoriesMetadata.repositories().stream().map(repositoryMetadata -> switch (randomInt(3)) {
            case 0 -> new RepositoryMetadata(randomIdentifier(), repositoryMetadata.uuid(), repositoryMetadata.type(), Settings.EMPTY);
            case 1 -> new RepositoryMetadata(repositoryMetadata.name(), randomUUID(), repositoryMetadata.type(), Settings.EMPTY);
            case 2 -> new RepositoryMetadata(
                repositoryMetadata.name(),
                repositoryMetadata.uuid(),
                randomAlphaOfLengthBetween(3, 8),
                Settings.EMPTY
            );
            default -> new RepositoryMetadata(
                repositoryMetadata.name(),
                repositoryMetadata.uuid(),
                repositoryMetadata.type(),
                Settings.builder().put("base_path", randomIdentifier()).build()
            );
        }).toList());
    }

    public static class TestBwcRepositoryMetadata extends AbstractNamedDiffable<Metadata.ClusterCustom> implements Metadata.ClusterCustom {

        private final List<RepositoryMetadata> repositories;

        public TestBwcRepositoryMetadata(List<RepositoryMetadata> repositories) {
            this.repositories = repositories;
        }

        public TestBwcRepositoryMetadata(StreamInput in) throws IOException {
            this.repositories = in.readCollectionAsImmutableList(RepositoryMetadata::new);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_GATEWAY;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.MINIMUM_COMPATIBLE;
        }

        @Override
        public String getWriteableName() {
            return RepositoriesMetadata.TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(repositories);
        }

        public static NamedDiff<Metadata.ClusterCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ClusterCustom.class, RepositoriesMetadata.TYPE, in);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return null;
        }
    }
}
