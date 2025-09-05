/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public class BlobContainerSupplier implements Supplier<BlobContainer> {

    private static final Logger logger = LogManager.getLogger(BlobContainerSupplier.class);

    private final Supplier<BlobStoreRepository> repositorySupplier;
    private final IndexId indexId;
    private final int shardId;

    private volatile LastKnownState lastKnownState = new LastKnownState(null, null);

    public BlobContainerSupplier(Supplier<BlobStoreRepository> repositorySupplier, IndexId indexId, int shardId) {
        this.repositorySupplier = repositorySupplier;
        this.indexId = indexId;
        this.shardId = shardId;
    }

    @Override
    public BlobContainer get() {
        final LastKnownState lastKnownState = this.lastKnownState;
        final BlobStoreRepository currentRepository = repositorySupplier.get();

        if (lastKnownState.blobStoreRepository() == currentRepository) {
            return lastKnownState.blobContainer();
        } else {
            return refreshAndGet();
        }
    }

    private synchronized BlobContainer refreshAndGet() {
        final BlobStoreRepository currentRepository = repositorySupplier.get();
        if (lastKnownState.blobStoreRepository() == currentRepository) {
            return lastKnownState.blobContainer();
        } else {
            logger.debug("creating new blob container [{}][{}][{}]", currentRepository.getMetadata().name(), indexId, shardId);
            final BlobContainer newContainer = new RateLimitingBlobContainer(
                currentRepository,
                currentRepository.shardContainer(indexId, shardId)
            );
            lastKnownState = new LastKnownState(currentRepository, newContainer);
            return newContainer;
        }
    }

    private record LastKnownState(BlobStoreRepository blobStoreRepository, BlobContainer blobContainer) {}

    /**
     * A {@link FilterBlobContainer} that uses {@link BlobStoreRepository#maybeRateLimitRestores(InputStream)} to limit the rate at which
     * blobs are read from the repository.
     */
    private static class RateLimitingBlobContainer extends FilterBlobContainer {

        private final BlobStoreRepository blobStoreRepository;

        RateLimitingBlobContainer(BlobStoreRepository blobStoreRepository, BlobContainer blobContainer) {
            super(blobContainer);
            this.blobStoreRepository = blobStoreRepository;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new RateLimitingBlobContainer(blobStoreRepository, child);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            return blobStoreRepository.maybeRateLimitRestores(super.readBlob(purpose, blobName));
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            return blobStoreRepository.maybeRateLimitRestores(super.readBlob(purpose, blobName, position, length));
        }
    }
}
