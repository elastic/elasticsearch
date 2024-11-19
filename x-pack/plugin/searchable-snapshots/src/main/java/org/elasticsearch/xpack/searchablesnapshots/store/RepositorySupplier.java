/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class RepositorySupplier implements Supplier<BlobStoreRepository> {

    private static final Logger logger = LogManager.getLogger(BlobContainerSupplier.class);

    private final RepositoriesService repositoriesService;

    private final String repositoryName;

    @Nullable // if repository specified only by name
    private final String repositoryUuid;

    private volatile String repositoryNameHint;

    public RepositorySupplier(RepositoriesService repositoriesService, String repositoryName, String repositoryUuid) {
        this.repositoriesService = Objects.requireNonNull(repositoriesService);
        this.repositoryName = Objects.requireNonNull(repositoryName);
        this.repositoryUuid = repositoryUuid;
        this.repositoryNameHint = repositoryName;
    }

    @Override
    public BlobStoreRepository get() {
        return SearchableSnapshots.getSearchableRepository(getRepository());
    }

    private Repository getRepository() {
        if (repositoryUuid == null) {
            // repository containing pre-7.12 snapshots has no UUID so we assume it matches by name
            final Repository repository = repositoriesService.repository(repositoryName);
            assert repository.getMetadata().name().equals(repositoryName) : repository.getMetadata().name() + " vs " + repositoryName;
            return repository;
        }

        final Map<String, Repository> repositoriesByName = repositoriesService.getRepositories();

        final String currentRepositoryNameHint = repositoryNameHint;
        final Repository repositoryByLastKnownName = repositoriesByName.get(currentRepositoryNameHint);
        if (repositoryByLastKnownName != null) {
            final var foundRepositoryUuid = repositoryByLastKnownName.getMetadata().uuid();
            if (Objects.equals(repositoryUuid, foundRepositoryUuid)) {
                return repositoryByLastKnownName;
            }
        }

        for (final Repository repository : repositoriesByName.values()) {
            if (repository.getMetadata().uuid().equals(repositoryUuid)) {
                final var newRepositoryName = repository.getMetadata().name();
                logger.debug(
                    "getRepository: repository [{}] with uuid [{}] replacing repository [{}]",
                    newRepositoryName,
                    repositoryUuid,
                    currentRepositoryNameHint
                );
                repositoryNameHint = repository.getMetadata().name();
                return repository;
            }
        }

        throw new RepositoryMissingException("uuid [" + repositoryUuid + "], original name [" + repositoryName + "]");
    }
}
