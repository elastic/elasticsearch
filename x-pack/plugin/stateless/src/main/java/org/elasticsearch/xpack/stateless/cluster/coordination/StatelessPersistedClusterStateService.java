/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class StatelessPersistedClusterStateService extends PersistedClusterStateService {
    private static final String CLUSTER_STATE_STAGING_DIR_NAME = "_tmp_state";
    private final ThreadPool threadPool;
    private final NodeEnvironment nodeEnvironment;
    private final ClusterSettings clusterSettings;
    private final SetOnce<LongSupplier> currentTermSupplier = new SetOnce<>();
    private final Supplier<ObjectStoreService> objectStoreServiceSupplier;

    private final Supplier<StatelessElectionStrategy> electionStrategySupplier;
    private final CompatibilityVersions compatibilityVersions;

    public StatelessPersistedClusterStateService(
        NodeEnvironment nodeEnvironment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeMillisSupplier,
        Supplier<StatelessElectionStrategy> electionStrategySupplier,
        Supplier<ObjectStoreService> objectStoreServiceSupplier,
        ThreadPool threadPool,
        CompatibilityVersions compatibilityVersions
    ) {
        super(nodeEnvironment, namedXContentRegistry, clusterSettings, relativeTimeMillisSupplier);
        this.objectStoreServiceSupplier = objectStoreServiceSupplier;
        this.threadPool = threadPool;
        this.nodeEnvironment = nodeEnvironment;
        this.clusterSettings = clusterSettings;
        this.electionStrategySupplier = electionStrategySupplier;
        this.compatibilityVersions = compatibilityVersions;
    }

    @Override
    protected Directory createDirectory(Path path) throws IOException {
        return new BlobStoreSyncDirectory(
            super.createDirectory(path),
            this::getBlobContainerForCurrentTerm,
            threadPool.executor(getClusterStateUploadsThreadPool())
        );
    }

    protected String getClusterStateUploadsThreadPool() {
        return Stateless.CLUSTER_STATE_READ_WRITE_THREAD_POOL;
    }

    protected String getClusterStateDownloadsThreadPool() {
        return Stateless.CLUSTER_STATE_READ_WRITE_THREAD_POOL;
    }

    private BlobContainer getBlobContainerForCurrentTerm() {
        assert currentTermSupplier.get() != null;
        return objectStoreService().getClusterStateBlobContainerForTerm(currentTermSupplier.get().getAsLong());
    }

    public CoordinationState.PersistedState createPersistedState(Settings settings, DiscoveryNode localNode) throws IOException {
        var persistedState = new StatelessPersistedState(
            this,
            objectStoreService()::getClusterStateBlobContainerForTerm,
            threadPool.executor(getClusterStateDownloadsThreadPool()),
            getStateStagingPath(),
            getInitialState(settings, localNode, clusterSettings, compatibilityVersions),
            Objects.requireNonNull(electionStrategySupplier.get())
        );
        currentTermSupplier.set(persistedState::getCurrentTerm);
        return persistedState;
    }

    Path getStateStagingPath() {
        var dataPath = nodeEnvironment.nodeDataPaths()[0];
        return dataPath.resolve(CLUSTER_STATE_STAGING_DIR_NAME);
    }

    private ObjectStoreService objectStoreService() {
        return Objects.requireNonNull(objectStoreServiceSupplier.get());
    }

    private static ClusterState getInitialState(
        Settings settings,
        DiscoveryNode localNode,
        ClusterSettings clusterSettings,
        CompatibilityVersions compatibilityVersions
    ) {
        return Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, localNode, compatibilityVersions))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterSettings))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .andThen(state -> addLocalNodeVotingConfig(state, localNode))
            .andThen(StatelessPersistedClusterStateService::withClusterUUID)
            .apply(ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings)).build());
    }

    private static ClusterState addLocalNodeVotingConfig(ClusterState clusterState, DiscoveryNode localNode) {
        var localNodeVotingConfiguration = CoordinationMetadata.VotingConfiguration.of(localNode);
        return ClusterState.builder(clusterState)
            .metadata(
                clusterState.metadata()
                    .withCoordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(localNodeVotingConfiguration)
                            .lastAcceptedConfiguration(localNodeVotingConfiguration)
                            .build()
                    )
            )
            .build();
    }

    private static ClusterState withClusterUUID(ClusterState clusterState) {
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).generateClusterUuidIfNeeded().build())
            .build();
    }
}
