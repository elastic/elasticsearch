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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 */
public class RepositoriesService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RepositoriesService.class);

    private final Map<String, Repository.Factory> typesRegistry;
    private final Map<String, Repository.Factory> internalTypesRegistry;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final VerifyNodeRepositoryAction verifyAction;

    private final Map<String, Repository> internalRepositories = ConcurrentCollections.newConcurrentMap();
    private volatile Map<String, Repository> repositories = Collections.emptyMap();

    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService,
                               Map<String, Repository.Factory> typesRegistry, Map<String, Repository.Factory> internalTypesRegistry,
                               ThreadPool threadPool) {
        this.typesRegistry = typesRegistry;
        this.internalTypesRegistry = internalTypesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.addStateApplier(this);
        }
        this.verifyAction = new VerifyNodeRepositoryAction(transportService, clusterService, this);
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
    public void registerRepository(final PutRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";

        final RepositoryMetaData newRepositoryMetaData = new RepositoryMetaData(request.name(), request.type(), request.settings());
        validate(request.name());

        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        if (request.verify()) {
            registrationListener = ActionListener.delegateFailure(listener, (delegatedListener, clusterStateUpdateResponse) -> {
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    // The response was acknowledged - all nodes should know about the new repository, let's verify them
                    verifyRepository(request.name(), ActionListener.delegateFailure(delegatedListener,
                        (innerDelegatedListener, discoveryNodes) -> innerDelegatedListener.onResponse(clusterStateUpdateResponse)));
                } else {
                    delegatedListener.onResponse(clusterStateUpdateResponse);
                }
            });
        } else {
            registrationListener = listener;
        }

        // Trying to create the new repository on master to make sure it works
        try {
            closeRepository(createRepository(newRepositoryMetaData, typesRegistry));
        } catch (Exception e) {
            registrationListener.onFailure(e);
            return;
        }

        clusterService.submitStateUpdateTask("put_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask<>(request, registrationListener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    ensureRepositoryNotInUse(currentState, request.name());
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    RepositoriesMetaData repositories = metaData.custom(RepositoriesMetaData.TYPE);
                    if (repositories == null) {
                        logger.info("put repository [{}]", request.name());
                        repositories = new RepositoriesMetaData(
                            Collections.singletonList(new RepositoryMetaData(request.name(), request.type(), request.settings())));
                    } else {
                        boolean found = false;
                        List<RepositoryMetaData> repositoriesMetaData = new ArrayList<>(repositories.repositories().size() + 1);

                        for (RepositoryMetaData repositoryMetaData : repositories.repositories()) {
                            if (repositoryMetaData.name().equals(newRepositoryMetaData.name())) {
                                if (newRepositoryMetaData.equalsIgnoreGenerations(repositoryMetaData)) {
                                    // Previous version is the same as this one no update is needed.
                                    return currentState;
                                }
                                found = true;
                                repositoriesMetaData.add(newRepositoryMetaData);
                            } else {
                                repositoriesMetaData.add(repositoryMetaData);
                            }
                        }
                        if (!found) {
                            logger.info("put repository [{}]", request.name());
                            repositoriesMetaData.add(new RepositoryMetaData(request.name(), request.type(), request.settings()));
                        } else {
                            logger.info("update repository [{}]", request.name());
                        }
                        repositories = new RepositoriesMetaData(repositoriesMetaData);
                    }
                    mdBuilder.putCustom(RepositoriesMetaData.TYPE, repositories);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name()), e);
                    super.onFailure(source, e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository is created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
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
    public void unregisterRepository(final DeleteRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("delete_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask<>(request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    ensureRepositoryNotInUse(currentState, request.name());
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    RepositoriesMetaData repositories = metaData.custom(RepositoriesMetaData.TYPE);
                    if (repositories != null && repositories.repositories().size() > 0) {
                        List<RepositoryMetaData> repositoriesMetaData = new ArrayList<>(repositories.repositories().size());
                        boolean changed = false;
                        for (RepositoryMetaData repositoryMetaData : repositories.repositories()) {
                            if (Regex.simpleMatch(request.name(), repositoryMetaData.name())) {
                                logger.info("delete repository [{}]", repositoryMetaData.name());
                                changed = true;
                            } else {
                                repositoriesMetaData.add(repositoryMetaData);
                            }
                        }
                        if (changed) {
                            repositories = new RepositoriesMetaData(repositoriesMetaData);
                            mdBuilder.putCustom(RepositoriesMetaData.TYPE, repositories);
                            return ClusterState.builder(currentState).metaData(mdBuilder).build();
                        }
                    }
                    if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                        return currentState;
                    }
                    throw new RepositoryMissingException(request.name());
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository was created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
                }
            });
    }

    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        final Repository repository = repository(repositoryName);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        verifyAction.verify(repositoryName, verificationToken, ActionListener.delegateFailure(listener,
                            (delegatedListener, verifyResponse) -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                try {
                                    repository.endVerification(verificationToken);
                                } catch (Exception e) {
                                    logger.warn(() -> new ParameterizedMessage(
                                        "[{}] failed to finish repository verification", repositoryName), e);
                                    delegatedListener.onFailure(e);
                                    return;
                                }
                                delegatedListener.onResponse(verifyResponse);
                            })));
                    } catch (Exception e) {
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                            try {
                                repository.endVerification(verificationToken);
                            } catch (Exception inner) {
                                inner.addSuppressed(e);
                                logger.warn(() -> new ParameterizedMessage(
                                    "[{}] failed to finish repository verification", repositoryName), inner);
                            }
                            listener.onFailure(e);
                        });
                    }
                } else {
                    listener.onResponse(Collections.emptyList());
                }
            }
        });
    }


    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            final ClusterState state = event.state();
            RepositoriesMetaData oldMetaData = event.previousState().getMetaData().custom(RepositoriesMetaData.TYPE);
            RepositoriesMetaData newMetaData = state.getMetaData().custom(RepositoriesMetaData.TYPE);

            // Check if repositories got changed
            if ((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equalsIgnoreGenerations(newMetaData))) {
                for (Repository repo : repositories.values()) {
                    repo.updateState(state);
                }
                return;
            }

            logger.trace("processing new index repositories for state version [{}]", event.state().version());

            Map<String, Repository> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                if (newMetaData == null || newMetaData.repository(entry.getKey()) == null) {
                    logger.debug("unregistering repository [{}]", entry.getKey());
                    closeRepository(entry.getValue());
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, Repository> builder = new HashMap<>();
            if (newMetaData != null) {
                // Now go through all repositories and update existing or create missing
                for (RepositoryMetaData repositoryMetaData : newMetaData.repositories()) {
                    Repository repository = survivors.get(repositoryMetaData.name());
                    if (repository != null) {
                        // Found previous version of this repository
                        RepositoryMetaData previousMetadata = repository.getMetadata();
                        if (previousMetadata.type().equals(repositoryMetaData.type()) == false
                            || previousMetadata.settings().equals(repositoryMetaData.settings()) == false) {
                            // Previous version is different from the version in settings
                            logger.debug("updating repository [{}]", repositoryMetaData.name());
                            closeRepository(repository);
                            repository = null;
                            try {
                                repository = createRepository(repositoryMetaData, typesRegistry);
                            } catch (RepositoryException ex) {
                                // TODO: this catch is bogus, it means the old repo is already closed,
                                // but we have nothing to replace it
                                logger.warn(() -> new ParameterizedMessage("failed to change repository [{}]",
                                    repositoryMetaData.name()), ex);
                            }
                        }
                    } else {
                        try {
                            repository = createRepository(repositoryMetaData, typesRegistry);
                        } catch (RepositoryException ex) {
                            logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetaData.name()), ex);
                        }
                    }
                    if (repository != null) {
                        logger.debug("registering repository [{}]", repositoryMetaData.name());
                        builder.put(repositoryMetaData.name(), repository);
                    }
                }
            }
            for (Repository repo : builder.values()) {
                repo.updateState(state);
            }
            repositories = Collections.unmodifiableMap(builder);
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
        Repository repository = repositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        repository = internalRepositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    public void registerInternalRepository(String name, String type) {
        RepositoryMetaData metaData = new RepositoryMetaData(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.computeIfAbsent(name, (n) -> {
            logger.debug("put internal repository [{}][{}]", name, type);
            return createRepository(metaData, internalTypesRegistry);
        });
        if (type.equals(repository.getMetadata().type()) == false) {
            logger.warn(new ParameterizedMessage("internal repository [{}][{}] already registered. this prevented the registration of " +
                "internal repository [{}][{}].", name, repository.getMetadata().type(), name, type));
        } else if (repositories.containsKey(name)) {
            logger.warn(new ParameterizedMessage("non-internal repository [{}] already registered. this repository will block the " +
                "usage of internal repository [{}][{}].", name, metaData.type(), name));
        }
    }

    public void unregisterInternalRepository(String name) {
        Repository repository = internalRepositories.remove(name);
        if (repository != null) {
            RepositoryMetaData metadata = repository.getMetadata();
            logger.debug(() -> new ParameterizedMessage("delete internal repository [{}][{}].", metadata.type(), name));
            closeRepository(repository);
        }
    }

    /** Closes the given repository. */
    private void closeRepository(Repository repository) {
        logger.debug("closing repository [{}][{}]", repository.getMetadata().type(), repository.getMetadata().name());
        repository.close();
    }

    /**
     * Creates repository holder. This method starts the repository
     */
    private Repository createRepository(RepositoryMetaData repositoryMetaData, Map<String, Repository.Factory> factories) {
        logger.debug("creating repository [{}][{}]", repositoryMetaData.type(), repositoryMetaData.name());
        Repository.Factory factory = factories.get(repositoryMetaData.type());
        if (factory == null) {
            throw new RepositoryException(repositoryMetaData.name(),
                "repository type [" + repositoryMetaData.type() + "] does not exist");
        }
        Repository repository = null;
        try {
            repository = factory.create(repositoryMetaData, factories::get);
            repository.start();
            return repository;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(repository);
            logger.warn(new ParameterizedMessage("failed to create repository [{}][{}]",
                repositoryMetaData.type(), repositoryMetaData.name()), e);
            throw new RepositoryException(repositoryMetaData.name(), "failed to create repository", e);
        }
    }

    private static void validate(final String repositoryName) {
        if (Strings.hasLength(repositoryName) == false) {
            throw new RepositoryException(repositoryName, "cannot be empty");
        }
        if (repositoryName.contains("#")) {
            throw new RepositoryException(repositoryName, "must not contain '#'");
        }
        if (Strings.validFileName(repositoryName) == false) {
            throw new RepositoryException(repositoryName,
                "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    private static void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (SnapshotsService.isRepositoryInUse(clusterState, repository) || RestoreService.isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used ");
        }
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        clusterService.removeApplier(this);
        final Collection<Repository> repos = new ArrayList<>();
        repos.addAll(internalRepositories.values());
        repos.addAll(repositories.values());
        IOUtils.close(repos);
    }
}
