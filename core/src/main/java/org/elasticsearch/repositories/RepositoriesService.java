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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 */
public class RepositoriesService extends AbstractComponent implements ClusterStateListener {

    private final RepositoryTypesRegistry typesRegistry;

    private final Injector injector;

    private final ClusterService clusterService;

    private final VerifyNodeRepositoryAction verifyAction;

    private volatile Map<String, RepositoryHolder> repositories = emptyMap();

    @Inject
    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService, RepositoryTypesRegistry typesRegistry, Injector injector) {
        super(settings);
        this.typesRegistry = typesRegistry;
        this.injector = injector;
        this.clusterService = clusterService;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.add(this);
        }
        this.verifyAction = new VerifyNodeRepositoryAction(settings, transportService, clusterService, this);
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
        final RepositoryMetaData newRepositoryMetaData = new RepositoryMetaData(request.name, request.type, request.settings);

        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        if (request.verify) {
            registrationListener = new VerifyingRegisterRepositoryListener(request.name, listener);
        } else {
            registrationListener = listener;
        }

        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, registrationListener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws IOException {
                ensureRepositoryNotInUse(currentState, request.name);
                // Trying to create the new repository on master to make sure it works
                if (!registerRepository(newRepositoryMetaData)) {
                    // The new repository has the same settings as the old one - ignore
                    return currentState;
                }
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                RepositoriesMetaData repositories = metaData.custom(RepositoriesMetaData.TYPE);
                if (repositories == null) {
                    logger.info("put repository [{}]", request.name);
                    repositories = new RepositoriesMetaData(new RepositoryMetaData(request.name, request.type, request.settings));
                } else {
                    boolean found = false;
                    List<RepositoryMetaData> repositoriesMetaData = new ArrayList<>(repositories.repositories().size() + 1);

                    for (RepositoryMetaData repositoryMetaData : repositories.repositories()) {
                        if (repositoryMetaData.name().equals(newRepositoryMetaData.name())) {
                            found = true;
                            repositoriesMetaData.add(newRepositoryMetaData);
                        } else {
                            repositoriesMetaData.add(repositoryMetaData);
                        }
                    }
                    if (!found) {
                        logger.info("put repository [{}]", request.name);
                        repositoriesMetaData.add(new RepositoryMetaData(request.name, request.type, request.settings));
                    } else {
                        logger.info("update repository [{}]", request.name);
                    }
                    repositories = new RepositoriesMetaData(repositoriesMetaData.toArray(new RepositoryMetaData[repositoriesMetaData.size()]));
                }
                mdBuilder.putCustom(RepositoriesMetaData.TYPE, repositories);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to create repository [{}]", t, request.name);
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.isMasterNode();
            }
        });
    }
    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the master node. It removes repository information from cluster metadata.
     *
     * @param request  unregister repository request
     * @param listener unregister repository listener
     */
    public void unregisterRepository(final UnregisterRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                ensureRepositoryNotInUse(currentState, request.name);
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                RepositoriesMetaData repositories = metaData.custom(RepositoriesMetaData.TYPE);
                if (repositories != null && repositories.repositories().size() > 0) {
                    List<RepositoryMetaData> repositoriesMetaData = new ArrayList<>(repositories.repositories().size());
                    boolean changed = false;
                    for (RepositoryMetaData repositoryMetaData : repositories.repositories()) {
                        if (Regex.simpleMatch(request.name, repositoryMetaData.name())) {
                            logger.info("delete repository [{}]", repositoryMetaData.name());
                            changed = true;
                        } else {
                            repositoriesMetaData.add(repositoryMetaData);
                        }
                    }
                    if (changed) {
                        repositories = new RepositoriesMetaData(repositoriesMetaData.toArray(new RepositoryMetaData[repositoriesMetaData.size()]));
                        mdBuilder.putCustom(RepositoriesMetaData.TYPE, repositories);
                        return ClusterState.builder(currentState).metaData(mdBuilder).build();
                    }
                }
                if (Regex.isMatchAllPattern(request.name)) { // we use a wildcard so we don't barf if it's not present.
                    return currentState;
                }
                throw new RepositoryMissingException(request.name);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // Since operation occurs only on masters, it's enough that only master-eligible nodes acked
                return discoveryNode.isMasterNode();
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
                            } catch (Throwable t) {
                                logger.warn("[{}] failed to finish repository verification", t, repositoryName);
                                listener.onFailure(t);
                                return;
                            }
                            listener.onResponse(verifyResponse);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            listener.onFailure(e);
                        }
                    });
                } catch (Throwable t) {
                    try {
                        repository.endVerification(verificationToken);
                    } catch (Throwable t1) {
                        logger.warn("[{}] failed to finish repository verification", t1, repositoryName);
                    }
                    listener.onFailure(t);
                }
            } else {
                listener.onResponse(new VerifyResponse(new DiscoveryNode[0], new VerificationFailure[0]));
            }
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }


    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            RepositoriesMetaData oldMetaData = event.previousState().getMetaData().custom(RepositoriesMetaData.TYPE);
            RepositoriesMetaData newMetaData = event.state().getMetaData().custom(RepositoriesMetaData.TYPE);

            // Check if repositories got changed
            if ((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equals(newMetaData))) {
                return;
            }

            logger.trace("processing new index repositories for state version [{}]", event.state().version());

            Map<String, RepositoryHolder> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, RepositoryHolder> entry : repositories.entrySet()) {
                if (newMetaData == null || newMetaData.repository(entry.getKey()) == null) {
                    logger.debug("unregistering repository [{}]", entry.getKey());
                    closeRepository(entry.getKey(), entry.getValue());
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, RepositoryHolder> builder = new HashMap<>();
            if (newMetaData != null) {
                // Now go through all repositories and update existing or create missing
                for (RepositoryMetaData repositoryMetaData : newMetaData.repositories()) {
                    RepositoryHolder holder = survivors.get(repositoryMetaData.name());
                    if (holder != null) {
                        // Found previous version of this repository
                        if (!holder.type.equals(repositoryMetaData.type()) || !holder.settings.equals(repositoryMetaData.settings())) {
                            // Previous version is different from the version in settings
                            logger.debug("updating repository [{}]", repositoryMetaData.name());
                            closeRepository(repositoryMetaData.name(), holder);
                            holder = null;
                            try {
                                holder = createRepositoryHolder(repositoryMetaData);
                            } catch (RepositoryException ex) {
                                logger.warn("failed to change repository [{}]", ex, repositoryMetaData.name());
                            }
                        }
                    } else {
                        try {
                            holder = createRepositoryHolder(repositoryMetaData);
                        } catch (RepositoryException ex) {
                            logger.warn("failed to create repository [{}]", ex, repositoryMetaData.name());
                        }
                    }
                    if (holder != null) {
                        logger.debug("registering repository [{}]", repositoryMetaData.name());
                        builder.put(repositoryMetaData.name(), holder);
                    }
                }
            }
            repositories = unmodifiableMap(builder);
        } catch (Throwable ex) {
            logger.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Returns registered repository
     * <p>
     * This method is called only on the master node
     *
     * @param repository repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(String repository) {
        RepositoryHolder holder = repositories.get(repository);
        if (holder != null) {
            return holder.repository;
        }
        throw new RepositoryMissingException(repository);
    }

    /**
     * Returns registered index shard repository
     * <p>
     * This method is called only on data nodes
     *
     * @param repository repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public IndexShardRepository indexShardRepository(String repository) {
        RepositoryHolder holder = repositories.get(repository);
        if (holder != null) {
            return holder.indexShardRepository;
        }
        throw new RepositoryMissingException(repository);
    }

    /**
     * Creates a new repository and adds it to the list of registered repositories.
     * <p>
     * If a repository with the same name but different types or settings already exists, it will be closed and
     * replaced with the new repository. If a repository with the same name exists but it has the same type and settings
     * the new repository is ignored.
     *
     * @param repositoryMetaData new repository metadata
     * @return {@code true} if new repository was added or {@code false} if it was ignored
     */
    private boolean registerRepository(RepositoryMetaData repositoryMetaData) throws IOException {
        RepositoryHolder previous = repositories.get(repositoryMetaData.name());
        if (previous != null) {
            if (!previous.type.equals(repositoryMetaData.type()) && previous.settings.equals(repositoryMetaData.settings())) {
                // Previous version is the same as this one - ignore it
                return false;
            }
        }
        RepositoryHolder holder = createRepositoryHolder(repositoryMetaData);
        if (previous != null) {
            // Closing previous version
            closeRepository(repositoryMetaData.name(), previous);
        }
        Map<String, RepositoryHolder> newRepositories = new HashMap<>(repositories);
        newRepositories.put(repositoryMetaData.name(), holder);
        return true;
    }

    /**
     * Closes the repository
     *
     * @param name   repository name
     * @param holder repository holder
     */
    private void closeRepository(String name, RepositoryHolder holder) throws IOException {
        logger.debug("closing repository [{}][{}]", holder.type, name);
        if (holder.repository != null) {
            holder.repository.close();
        }
    }

    /**
     * Creates repository holder
     */
    private RepositoryHolder createRepositoryHolder(RepositoryMetaData repositoryMetaData) {
        logger.debug("creating repository [{}][{}]", repositoryMetaData.type(), repositoryMetaData.name());
        Injector repositoryInjector = null;
        try {
            ModulesBuilder modules = new ModulesBuilder();
            RepositoryName name = new RepositoryName(repositoryMetaData.type(), repositoryMetaData.name());
            modules.add(new RepositoryNameModule(name));
            modules.add(new RepositoryModule(name, repositoryMetaData.settings(), this.settings, typesRegistry));

            repositoryInjector = modules.createChildInjector(injector);
            Repository repository = repositoryInjector.getInstance(Repository.class);
            IndexShardRepository indexShardRepository = repositoryInjector.getInstance(IndexShardRepository.class);
            repository.start();
            return new RepositoryHolder(repositoryMetaData.type(), repositoryMetaData.settings(), repositoryInjector, repository, indexShardRepository);
        } catch (Throwable t) {
            logger.warn("failed to create repository [{}][{}]", t, repositoryMetaData.type(), repositoryMetaData.name());
            throw new RepositoryException(repositoryMetaData.name(), "failed to create repository", t);
        }
    }

    private void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (SnapshotsService.isRepositoryInUse(clusterState, repository) || RestoreService.isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used ");
        }
    }

    private class VerifyingRegisterRepositoryListener implements ActionListener<ClusterStateUpdateResponse> {

        private final String name;

        private final ActionListener<ClusterStateUpdateResponse> listener;

        public VerifyingRegisterRepositoryListener(String name, final ActionListener<ClusterStateUpdateResponse> listener) {
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
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });
            } else {
                listener.onResponse(clusterStateUpdateResponse);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(e);
        }
    }

    /**
     * Internal data structure for holding repository with its configuration information and injector
     */
    private static class RepositoryHolder {

        private final String type;
        private final Settings settings;
        private final Repository repository;
        private final IndexShardRepository indexShardRepository;

        public RepositoryHolder(String type, Settings settings, Injector injector, Repository repository, IndexShardRepository indexShardRepository) {
            this.type = type;
            this.settings = settings;
            this.repository = repository;
            this.indexShardRepository = indexShardRepository;
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

        Settings settings = EMPTY_SETTINGS;

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

}
