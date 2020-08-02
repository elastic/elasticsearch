/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryInfo;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;

import java.time.Instant;
import java.util.Optional;

public abstract class MeteredBlobStoreRepository extends BlobStoreRepository {
    private final String ephemeralId;
    private final Instant startedAt;

    public MeteredBlobStoreRepository(RepositoryMetadata metadata,
                                      NamedXContentRegistry namedXContentRegistry,
                                      ClusterService clusterService,
                                      RecoverySettings recoverySettings,
                                      BlobPath basePath) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings, basePath);
        this.ephemeralId = UUIDs.randomBase64UUID();
        this.startedAt = Instant.now();
    }

    @Override
    public Optional<RepositoryStatsSnapshot> statsSnapshot() {
        RepositoryInfo repositoryInfo = new RepositoryInfo(ephemeralId, metadata.name(), metadata.type(), location(), startedAt);
        return Optional.of(new RepositoryStatsSnapshot(repositoryInfo, stats()));
    }

    protected abstract String location();
}
