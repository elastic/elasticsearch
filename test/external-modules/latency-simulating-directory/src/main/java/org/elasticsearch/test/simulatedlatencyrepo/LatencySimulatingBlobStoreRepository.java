/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.simulatedlatencyrepo;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;

class LatencySimulatingBlobStoreRepository extends FsRepository {

    private final Runnable simulator;

    protected LatencySimulatingBlobStoreRepository(
        ProjectId projectId,
        RepositoryMetadata metadata,
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        Runnable simulator
    ) {
        super(projectId, metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        this.simulator = simulator;
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        BlobStore fsBlobStore = super.createBlobStore();
        return new BlobStore() {
            @Override
            public BlobContainer blobContainer(BlobPath path) {
                BlobContainer blobContainer = fsBlobStore.blobContainer(path);
                return new LatencySimulatingBlobContainer(blobContainer);
            }

            @Override
            public void close() throws IOException {
                fsBlobStore.close();
            }
        };
    }

    private class LatencySimulatingBlobContainer extends FilterBlobContainer {

        LatencySimulatingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            simulator.run();
            return super.readBlob(purpose, blobName);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            simulator.run();
            return super.readBlob(purpose, blobName, position, length);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new LatencySimulatingBlobContainer(child);
        }
    }
}
