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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RepositoriesMetadataSerializationTests extends ChunkedToXContentDiffableSerializationTestCase<Custom> {

    @Override
    protected Custom createTestInstance() {
        int numberOfRepositories = randomInt(10);
        List<RepositoryMetadata> entries = new ArrayList<>();
        for (int i = 0; i < numberOfRepositories; i++) {
            // divide by 2 to not overflow when adding to this number for the pending generation below
            final long generation = randomNonNegativeLong() / 2L;
            entries.add(
                new RepositoryMetadata(
                    randomAlphaOfLength(10),
                    randomAlphaOfLength(10),
                    randomAlphaOfLength(10),
                    randomSettings(),
                    generation,
                    generation + randomLongBetween(0, generation)
                )
            );
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
        List<RepositoryMetadata> entries = new ArrayList<>(((RepositoriesMetadata) instance).repositories());
        boolean addEntry = entries.isEmpty() ? true : randomBoolean();
        if (addEntry) {
            entries.add(new RepositoryMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomSettings()));
        } else {
            entries.remove(randomIntBetween(0, entries.size() - 1));
        }
        return new RepositoriesMetadata(entries);
    }

    public Settings randomSettings() {
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

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        RepositoriesMetadata repositoriesMetadata = (RepositoriesMetadata) testInstance;
        List<RepositoryMetadata> repos = new ArrayList<>(repositoriesMetadata.repositories());
        if (randomBoolean() && repos.size() > 1) {
            // remove some elements
            int leaveElements = randomIntBetween(0, repositoriesMetadata.repositories().size() - 1);
            repos = randomSubsetOf(leaveElements, repos.toArray(new RepositoryMetadata[leaveElements]));
        }
        if (randomBoolean()) {
            // add some elements
            int addElements = randomInt(10);
            for (int i = 0; i < addElements; i++) {
                repos.add(new RepositoryMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomSettings()));
            }
        }
        return new RepositoriesMetadata(repos);
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
}
