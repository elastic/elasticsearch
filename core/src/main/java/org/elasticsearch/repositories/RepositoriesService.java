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
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 */
public class RepositoriesService extends AbstractComponent implements ClusterStateApplier {

    private final Map<String, Repository.Factory> typesRegistry;

    private final ClusterService clusterService;

    private final VerifyNodeRepositoryAction verifyAction;

    private volatile Map<String, Repository> repositories = Collections.emptyMap();

    @Inject
    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService,
                               Map<String, Repository.Factory> typesRegistry) {
        super(settings);
        this.typesRegistry = typesRegistry;
        this.clusterService = clusterService;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.addStateApplier(this);
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
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to create repository [{}]", request.name), e);
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
                // repository was created on both master and data nodes
                return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
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
                                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName), e);
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
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName), inner);
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
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            RepositoriesMetaData oldMetaData = event.previousState().getMetaData().custom(RepositoriesMetaData.TYPE);
            RepositoriesMetaData newMetaData = event.state().getMetaData().custom(RepositoriesMetaData.TYPE);

            // Check if repositories got changed
            if ((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equals(newMetaData))) {
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
                                repository = createRepository(repositoryMetaData);
                            } catch (RepositoryException ex) {
                                // TODO: this catch is bogus, it means the old repo is already closed,
                                // but we have nothing to replace it
                                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to change repository [{}]", repositoryMetaData.name()), ex);
                            }
                        }
                    } else {
                        try {
                            repository = createRepository(repositoryMetaData);
                        } catch (RepositoryException ex) {
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetaData.name()), ex);
                        }
                    }
                    if (repository != null) {
                        logger.debug("registering repository [{}]", repositoryMetaData.name());
                        builder.put(repositoryMetaData.name(), repository);
                    }
                }
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
        throw new RepositoryMissingException(repositoryName);
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
        Repository previous = repositories.get(repositoryMetaData.name());
        if (previous != null) {
            RepositoryMetaData previousMetadata = previous.getMetadata();
            if (!previousMetadata.type().equals(repositoryMetaData.type()) && previousMetadata.settings().equals(repositoryMetaData.settings())) {
                // Previous version is the same as this one - ignore it
                return false;
            }
        }
        Repository newRepo = createRepository(repositoryMetaData);
        if (previous != null) {
            closeRepository(previous);
        }
        Map<String, Repository> newRepositories = new HashMap<>(repositories);
        newRepositories.put(repositoryMetaData.name(), newRepo);
        repositories = newRepositories;
        return true;
    }

    /** Closes the given repository. */
    private void closeRepository(Repository repository) throws IOException {
        logger.debug("closing repository [{}][{}]", repository.getMetadata().type(), repository.getMetadata().name());
        repository.close();
    }

    /**
     * Creates repository holder
     */
    private Repository createRepository(RepositoryMetaData repositoryMetaData) {
        logger.debug("creating repository [{}][{}]", repositoryMetaData.type(), repositoryMetaData.name());
        Repository.Factory factory = typesRegistry.get(repositoryMetaData.type());
        if (factory == null) {
            throw new RepositoryException(repositoryMetaData.name(),
                "repository type [" + repositoryMetaData.type() + "] does not exist");
        }
        try {
            Repository repository = factory.create(repositoryMetaData);
            repository.start();
            return repository;
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to create repository [{}][{}]", repositoryMetaData.type(), repositoryMetaData.name()), e);
            throw new RepositoryException(repositoryMetaData.name(), "failed to create repository", e);
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

}
