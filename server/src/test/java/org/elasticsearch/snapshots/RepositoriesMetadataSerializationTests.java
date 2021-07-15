/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RepositoriesMetadataSerializationTests extends AbstractDiffableSerializationTestCase<Custom> {

    /**
     * Repository names are used as field names in the serialized XContent and this can fail parsing
     * so we use a generator to have unique names.
     */
    private static final AtomicLong generator = new AtomicLong();

    @Override
    protected Custom createTestInstance() {
        final int numberOfRepositories = randomIntBetween(1, 20);
        final List<RepositoryMetadata> entries = new ArrayList<>();
        for (int i = 0; i < numberOfRepositories; i++) {
            entries.add(randomRepositoryMetadata());
        }
        entries.sort(Comparator.comparing(RepositoryMetadata::name));
        return new RepositoriesMetadata(entries);
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return RepositoriesMetadata::new;
    }

    @Override
    protected Custom mutateInstance(Custom instance) {
        final List<RepositoryMetadata> entries = new ArrayList<>(((RepositoriesMetadata) instance).repositories());
        if (entries.isEmpty() || randomBoolean()) {
            entries.add(randomRepositoryMetadata());
        } else if (randomBoolean()) {
            entries.remove(randomIntBetween(0, entries.size() - 1));
        } else {
            int index = randomIntBetween(0, entries.size() - 1);
            entries.add(index, mutateRepositoryMetadata(entries.get(index)));
        }
        return new RepositoriesMetadata(entries);
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return RepositoriesMetadata::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Custom doParseInstance(XContentParser parser) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        RepositoriesMetadata repositoriesMetadata = RepositoriesMetadata.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        List<RepositoryMetadata> repos = new ArrayList<>(repositoriesMetadata.repositories());
        repos.sort(Comparator.comparing(RepositoryMetadata::name));
        return new RepositoriesMetadata(repos);
    }

    private RepositoryMetadata mutateRepositoryMetadata(RepositoryMetadata instance) {
        if (randomBoolean()) {
            return instance.withUuid(randomValueOtherThan(instance.uuid(), UUIDs::randomBase64UUID));
        } else if (randomBoolean()) {
            return instance.withSettings(randomValueOtherThan(instance.settings(), this::randomSettings));
        } else if (instance.hasSnapshotsToDelete() == false || randomBoolean()) {
            final SnapshotId snapshotId = randomValueOtherThanMany(s -> instance.snapshotsToDelete().contains(s), this::randomSnapshotId);
            return instance.addSnapshotsToDelete(List.of(snapshotId));
        } else {
            return instance.removeSnapshotsToDelete(
                randomSubsetOf(randomIntBetween(0, instance.snapshotsToDelete().size() - 1), instance.snapshotsToDelete())
            );
        }
    }

    private RepositoryMetadata randomRepositoryMetadata() {
        String name = String.valueOf(generator.getAndIncrement()) + '-' + randomAlphaOfLengthBetween(1, 10);
        String type = randomAlphaOfLengthBetween(1, 10);
        Settings settings = randomSettings();
        if (randomBoolean()) {
            return new RepositoryMetadata(name, type, settings);
        }
        String uuid = UUIDs.randomBase64UUID();
        // divide by 2 to not overflow when adding to this number for the pending generation below
        long generation = randomNonNegativeLong() / 2L;
        long pendingGeneration = generation + randomLongBetween(0, generation);
        List<SnapshotId> snapshotsToDelete = randomSnapshotsToDelete();
        return new RepositoryMetadata(name, uuid, type, settings, generation, pendingGeneration, snapshotsToDelete);
    }

    private Settings randomSettings() {
        if (randomBoolean()) {
            return Settings.EMPTY;
        } else {
            int numberOfSettings = randomInt(10);
            Settings.Builder builder = Settings.builder();
            for (int i = 0; i < numberOfSettings; i++) {
                builder.put(randomAlphaOfLength(10), randomAlphaOfLength(20));
            }
            return builder.build();
        }
    }

    private List<SnapshotId> randomSnapshotsToDelete() {
        if (randomBoolean()) {
            return List.of();
        } else {
            final int numberOfSnapshots = randomIntBetween(1, 10);
            final List<SnapshotId> snapshotIds = new ArrayList<>(numberOfSnapshots);
            for (int i = 0; i < numberOfSnapshots; i++) {
                snapshotIds.add(randomSnapshotId());
            }
            return List.copyOf(snapshotIds);
        }
    }

    private SnapshotId randomSnapshotId() {
        return new SnapshotId(randomAlphaOfLengthBetween(1, 10), UUIDs.randomBase64UUID());
    }
}
