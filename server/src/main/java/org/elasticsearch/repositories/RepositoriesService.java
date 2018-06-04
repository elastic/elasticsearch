/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 */
public class RepositoriesService extends AbstractComponent implements ClusterStateApplier {

    private final Map<String, Repository.Factory> typesRegistry;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final VerifyNodeRepositoryAction verifyAction;
    private final Map<String, Repository> repositories;

    @Inject
    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService,
                               Map<String, Repository.Factory> typesRegistry, ThreadPool threadPool) {
        super(settings);
        this.typesRegistry = typesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.addStateApplier(this);
        }
        this.verifyAction = new VerifyNodeRepositoryAction(settings, transportService, clusterService, this);
        this.repositories = new ConcurrentHashMap<>();
    }

    /**
     * Registers new repository in the cluster
     * <p>
     * This method can be only called on the master node. It tries to create a new repository on the master
     * and if it was successful it adds new repository to cluster metadata.
     *
     * @param request  register repository request
     * @param listener register repository listener
     */
    public void registerRepository(final RegisterRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final RepositoryMetaData repositoryMetaData = new RepositoryMetaData(request.name, request.type, request.settings);

        // Fail fast if the repository is used
        // This is checked again when updating the cluster state
        final ClusterState clusterState = clusterService.state();
        ensureRepositoryNotInUse(clusterState, repositoryMetaData.name());

        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        if (request.verify) {
            registrationListener = new VerifyingRegisterRepositoryListener(request.name, listener);
        } else {
            registrationListener = listener;
        }

        // Check if a repository already exists with the same metadata
        if (hasRepositoryWithSameMetadata(clusterState, repositoryMetaData)) {
            // Same metadata, do nothing (but potentially triggers the verify)
            registrationListener.onResponse(new ClusterStateUpdateResponse(true));
            return;
        }

        try {
            Repository repository = null;
            try {
                // Create and start the repository on master node
                repository = createRepository(repositoryMetaData);

                final Repository finalRepository = repository;
                clusterService.submitStateUpdateTask(request.cause,
                    new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, registrationListener) {

                        private final List<Repository> closeables = new ArrayList<>(Collections.singleton(finalRepository));

                        @Override
                        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                            return new ClusterStateUpdateResponse(acknowledged);
                        }

                        @Override
                        public ClusterState execute(final ClusterState currentState) throws IOException {
                            // Repository must not be used
                            ensureRepositoryNotInUse(currentState, repositoryMetaData.name());

                            // Repository with same metadata already exists, just ignore
                            if (hasRepositoryWithSameMetadata(currentState, repositoryMetaData)) {
                                assert repositories.containsKey(repositoryMetaData.name());
                                return currentState;
                            }

                            repositories.compute(repositoryMetaData.name(), (name, previous) -> {
                                if (previous != null) {
                                    closeables.add(previous);
                                }
                                closeables.remove(finalRepository);
                                return finalRepository;
                            });

                            final List<RepositoryMetaData> repositoryMetaDataList = new ArrayList<>();

                            final RepositoriesMetaData repositoriesMetaData = currentState.metaData().custom(RepositoriesMetaData.TYPE);
                            if (repositoriesMetaData == null) {
                                logger.info("put repository [{}]", request.name);
                                repositoryMetaDataList.add(repositoryMetaData);
                            } else {
                                boolean found = false;
                                for (RepositoryMetaData existingRepositoryMetaData : repositoriesMetaData.repositories()) {
                                    if (existingRepositoryMetaData.name().equals(repositoryMetaData.name())) {
                                        found = true;
                                        repositoryMetaDataList.add(repositoryMetaData);
                                    } else {
                                        repositoryMetaDataList.add(existingRepositoryMetaData);
                                    }
                                }
                                if (found == false) {
                                    logger.info("put repository [{}]", request.name);
                                    repositoryMetaDataList.add(repositoryMetaData);
                                } else {
                                    logger.info("update repository [{}]", request.name);
                                }
                            }

                            final MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
                            metaDataBuilder.putCustom(RepositoriesMetaData.TYPE, new RepositoriesMetaData(repositoryMetaDataList));
                            return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name), e);
                            super.onFailure(source, e);
                        }

                        @Override
                        public boolean mustAck(DiscoveryNode discoveryNode) {
                            // repository is created on both master and data nodes
                            return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
                        }

                        @Override
                        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                            closeRepositories(closeables);
                        }
                    });
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("failed to create repository [{}][{}]", request.type, request.name), e);
                if (repository != null) {
                    IOUtils.close(e, repository);
                }
                throw e;
            }
        } catch (Exception e) {
            registrationListener.onFailure(e);
        }
    }

    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the master node. It removes repository information from cluster metadata.
     *
     * @param request  unregister repository request
     * @param listener unregister repository listener
     */
    public void unregisterRepository(final UnregisterRepositoryRequest request,
                                     final ActionListener<ClusterStateUpdateResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        ensureRepositoryNotInUse(clusterState, request.name);

        clusterService.submitStateUpdateTask(request.cause,
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

                private final List<Repository> closeables = new ArrayList<>();

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    ensureRepositoryNotInUse(currentState, request.name);

                    final RepositoriesMetaData repositoriesMetaData = currentState.metaData().custom(RepositoriesMetaData.TYPE);
                    if (repositoriesMetaData != null && repositoriesMetaData.repositories().size() > 0) {
                        final List<RepositoryMetaData> repositoryMetaDataList = new ArrayList<>();
                        boolean changed = false;
                        for (RepositoryMetaData existingRepositoryMetaData : repositoriesMetaData.repositories()) {
                            if (Regex.simpleMatch(request.name, existingRepositoryMetaData.name())) {
                                logger.info("delete repository [{}]", existingRepositoryMetaData.name());
                                changed = true;

                                final Repository repository = repositories.remove(existingRepositoryMetaData.name());
                                assert repository != null : "Repository should exist";
                                closeables.add(repository);
                            } else {
                                repositoryMetaDataList.add(existingRepositoryMetaData);
                            }
                        }
                        if (changed) {
                            final MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
                            metaDataBuilder.putCustom(RepositoriesMetaData.TYPE, new RepositoriesMetaData(repositoryMetaDataList));
                            return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
                        }
                    }
                    // we use a wildcard so we don't barf if it's not present.
                    if (Regex.isMatchAllPattern(request.name)) {
                        return currentState;
                    }
                    throw new RepositoryMissingException(request.name);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository was created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
                }

                @Override
                public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                    closeRepositories(closeables);
                }
        });
    }

    public void verifyRepository(final String repositoryName, final ActionListener<VerifyResponse> listener) {
        final Repository repository = repository(repositoryName);
        try {
            final String verificationToken = repository.startVerification();
            if (verificationToken != null) {
                try {
                    verifyAction.verify(repositoryName, verificationToken, new ActionListener<VerifyResponse>() {
                        @Override
                        public void onResponse(VerifyResponse verifyResponse) {
                            try {
                                repository.endVerification(verificationToken);
                            } catch (Exception e) {
                                logger.warn(() -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName), e);
                                listener.onFailure(e);
                                return;
                            }
                            listener.onResponse(verifyResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                } catch (Exception e) {
                    try {
                        repository.endVerification(verificationToken);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName), inner);
                    }
                    listener.onFailure(e);
                }
            } else {
                listener.onResponse(new VerifyResponse(new DiscoveryNode[0], new VerificationFailure[0]));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     */
    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        try {
            final RepositoriesMetaData previousMetaData = event.previousState().getMetaData().custom(RepositoriesMetaData.TYPE);
            final RepositoriesMetaData currentMetaData = event.state().getMetaData().custom(RepositoriesMetaData.TYPE);

            if (Objects.equals(previousMetaData, currentMetaData)) {
                return;
            }

            final List<Repository> closeables = new ArrayList<>();

            final Map<String, Repository> survivors = new HashMap<>();
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                final String repositoryName = entry.getKey();
                if (currentMetaData == null || currentMetaData.repository(repositoryName) == null) {
                    logger.debug("unregistering repository [{}]", repositoryName);
                    repositories.compute(repositoryName, (name, previous) -> {
                        if (previous != null) {
                            closeables.add(previous);
                        }
                        return null;
                    });
                } else {
                    survivors.put(repositoryName, entry.getValue());
                }
            }

            if (currentMetaData != null) {
                for (RepositoryMetaData repositoryMetaData : currentMetaData.repositories()) {
                    final String repositoryName = repositoryMetaData.name();

                    final Repository existingRepository = survivors.get(repositoryName);
                    if ((existingRepository != null)
                        && (Objects.equals(existingRepository.getMetadata(), repositoryMetaData))
                        && !(existingRepository instanceof FailedRepository)) {
                        logger.debug(() -> new ParameterizedMessage("repository [{}] already exists with same metadata", repositoryName));
                        continue;
                    }

                    Repository repository = null;
                    try {
                        repository = createRepository(repositoryMetaData);
                    } catch (Exception ex) {
                        logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetaData.name()), ex);
                        repository = new FailedRepository(settings, repositoryMetaData);
                    }

                    Repository finalRepository = repository;
                    repositories.compute(repositoryName, (name, previous) -> {
                        if (previous != null) {
                            closeables.add(previous);
                        }
                        return finalRepository;
                    });

                    logger.debug("repository [{}] registered", repositoryName);
                }
            }
        } catch (Exception ex) {
            logger.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Returns registered repository
     * <p>
     * This method is called only on the master node
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(String repositoryName) {
        final Repository repository = repositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    /**
     * Returns the {@link Repository.Factory} to use to create a {@link Repository} with
     * the given name and type. A {@link RepositoryException} is thrown if
     * the repository type is unknown.
     *
     * @param repositoryName the name of the repository
     * @param repositoryType the type of the repository
     * @return the {@link Repository.Factory} to use to create the repository
     */
    private Repository.Factory getRepositoryFactory(final String repositoryName, final String repositoryType) {
        final Repository.Factory factory = typesRegistry.get(repositoryType);
        if (factory == null) {
            throw new RepositoryException(repositoryName, "repository type [" + repositoryType + "] does not exist");
        }
        return factory;
    }

    /**
     * Creates repository holder
     */
    private Repository createRepository(final RepositoryMetaData repositoryMetaData) {
        final String repositoryName = repositoryMetaData.name();
        final String repositoryType = repositoryMetaData.type();

        logger.debug("creating repository [{}][{}]", repositoryType, repositoryName);
        try {
            final Repository repository = getRepositoryFactory(repositoryName, repositoryType).create(repositoryMetaData);
            repository.start();
            return repository;
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("failed to create repository [{}][{}]", repositoryType, repositoryName), e);
            throw new RepositoryException(repositoryMetaData.name(), "failed to create repository", e);
        }
    }

    /**
     * Closes one or more {@link Repository}
     */
    private void closeRepositories(final List<Repository> repositories) {
        if (repositories == null || repositories.isEmpty()) {
            return;
        }
        threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(() -> IOUtils.closeWhileHandlingException(repositories));
    }

    private static void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (SnapshotsService.isRepositoryInUse(clusterState, repository) || RestoreService.isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used ");
        }
    }

    /**
     * Returns true if a repository with the same metadata already exists in the given cluster state.
     *
     * @param clusterState       the cluster state
     * @param repositoryMetaData the {@link RepositoryMetaData} to look fo
     * @return true if the {@link RepositoryMetaData} is found in the cluster state, false otherwise.
     */
    private static boolean hasRepositoryWithSameMetadata(final ClusterState clusterState, final RepositoryMetaData repositoryMetaData) {
        final RepositoriesMetaData repositories = clusterState.metaData().custom(RepositoriesMetaData.TYPE);
        if (repositories != null) {
            for (RepositoryMetaData repository : repositories.repositories()) {
                if (Objects.equals(repositoryMetaData, repository)) {
                    return true;
                }
            }
        }
        return false;
    }

    private class VerifyingRegisterRepositoryListener implements ActionListener<ClusterStateUpdateResponse> {

        private final String name;

        private final ActionListener<ClusterStateUpdateResponse> listener;

        VerifyingRegisterRepositoryListener(String name, final ActionListener<ClusterStateUpdateResponse> listener) {
            this.name = name;
            this.listener = listener;
        }

        @Override
        public void onResponse(final ClusterStateUpdateResponse clusterStateUpdateResponse) {
            if (clusterStateUpdateResponse.isAcknowledged()) {
                // The response was acknowledged - all nodes should know about the new repository, let's verify them
                verifyRepository(name, new ActionListener<VerifyResponse>() {
                    @Override
                    public void onResponse(VerifyResponse verifyResponse) {
                        if (verifyResponse.failed()) {
                            listener.onFailure(new RepositoryVerificationException(name, verifyResponse.failureDescription()));
                        } else {
                            listener.onResponse(clusterStateUpdateResponse);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            } else {
                listener.onResponse(clusterStateUpdateResponse);
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Register repository request
     */
    public static class RegisterRepositoryRequest extends ClusterStateUpdateRequest<RegisterRepositoryRequest> {

        final String cause;

        final String name;

        final String type;

        final boolean verify;

        Settings settings = Settings.EMPTY;

        /**
         * Constructs new register repository request
         *
         * @param cause repository registration cause
         * @param name  repository name
         * @param type  repository type
         * @param verify verify repository after creation
         */
        public RegisterRepositoryRequest(String cause, String name, String type, boolean verify) {
            this.cause = cause;
            this.name = name;
            this.type = type;
            this.verify = verify;
        }

        /**
         * Sets repository settings
         *
         * @param settings repository settings
         * @return this request
         */
        public RegisterRepositoryRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }
    }

    /**
     * Unregister repository request
     */
    public static class UnregisterRepositoryRequest extends ClusterStateUpdateRequest<UnregisterRepositoryRequest> {

        final String cause;

        final String name;

        /**
         * Creates a new unregister repository request
         *
         * @param cause repository unregistration cause
         * @param name  repository name
         */
        public UnregisterRepositoryRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

    }

    /**
     * Verify repository request
     */
    public static class VerifyResponse {

        private VerificationFailure[] failures;

        private DiscoveryNode[] nodes;

        public VerifyResponse(DiscoveryNode[] nodes, VerificationFailure[] failures) {
            this.nodes = nodes;
            this.failures = failures;
        }

        public VerificationFailure[] failures() {
            return failures;
        }

        public DiscoveryNode[] nodes() {
            return nodes;
        }

        public boolean failed() {
            return  failures.length > 0;
        }

        public String failureDescription() {
            return Arrays
                    .stream(failures)
                    .map(failure -> failure.toString())
                    .collect(Collectors.joining(", ", "[", "]"));
        }

    }

    class FailedRepository extends AbstractLifecycleComponent implements Repository {

        private final RepositoryMetaData repositoryMetaData;

        public FailedRepository(final Settings settings, final RepositoryMetaData repositoryMetaData) {
            super(settings);
            this.repositoryMetaData = repositoryMetaData;
        }

        @Override
        protected void doStart() {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() throws IOException {
        }

        @Override
        public RepositoryMetaData getMetadata() {
            return repositoryMetaData;
        }

        @Override
        public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public RepositoryData getRepositoryData() {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                             List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public String startVerification() {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public void endVerification(String verificationToken) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        @Override
        public void snapshotShard(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                                  IndexShardSnapshotStatus snapshotStatus) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId,
                                 RecoveryState recoveryState) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public long getSnapshotThrottleTimeInNanos() {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }

        @Override
        public long getRestoreThrottleTimeInNanos() {
            throw new UnsupportedOperationException("Repository " + repositoryMetaData.name() + " has failed to start on this node");
        }
    }
}
