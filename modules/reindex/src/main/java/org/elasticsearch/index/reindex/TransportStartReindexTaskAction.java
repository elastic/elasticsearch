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
package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportStartReindexTaskAction
    extends HandledTransportAction<StartReindexTaskAction.Request, StartReindexTaskAction.Response> {

    private final List<String> headersToInclude;
    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;
    private final ReindexValidator reindexValidator;
    private final ReindexIndexClient reindexIndexClient;

    @Inject
    public TransportStartReindexTaskAction(Settings settings, Client client, TransportService transportService, ThreadPool threadPool,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                           ClusterService clusterService, PersistentTasksService persistentTasksService,
                                           AutoCreateIndex autoCreateIndex, NamedXContentRegistry xContentRegistry) {
        super(StartReindexTaskAction.NAME, transportService, actionFilters, StartReindexTaskAction.Request::new);
        this.headersToInclude = ReindexHeaders.REINDEX_INCLUDED_HEADERS.get(settings);
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.persistentTasksService = persistentTasksService;
        this.reindexIndexClient = new ReindexIndexClient(client, clusterService, xContentRegistry);
    }

    @Override
    protected void doExecute(Task task, StartReindexTaskAction.Request request, ActionListener<StartReindexTaskAction.Response> listener) {
        try {
            reindexValidator.initialValidation(request.getReindexRequest());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        String generatedId = UUIDs.randomBase64UUID();

        ThreadContext threadContext = threadPool.getThreadContext();
        Map<String, String> included = headersToInclude.stream()
            .map(header -> new Tuple<>(header, threadContext.getHeader(header)))
            .filter(t -> t.v2() != null)
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        // In the current implementation, we only need to store task results if we do not wait for completion
        boolean storeTaskResult = request.getWaitForCompletion() == false;
        ReindexTaskParams job = new ReindexTaskParams(storeTaskResult, included);

        ReindexTaskStateDoc reindexState =
                new ReindexTaskStateDoc(request.getReindexRequest(), resolveIndexPatterns(request.getReindexRequest()));
        reindexIndexClient.createReindexTaskDoc(generatedId, reindexState, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                persistentTasksService.sendStartRequest(generatedId, ReindexTask.NAME, job, new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexTaskParams> persistentTask) {
                        if (request.getWaitForCompletion()) {
                            waitForReindexDone(persistentTask.getId(), listener);
                        } else {
                            waitForReindexTask(persistentTask.getId(), listener);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assert e instanceof ResourceAlreadyExistsException == false : "UUID generation should not produce conflicts";
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void waitForReindexDone(String taskId, ActionListener<StartReindexTaskAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexPredicate(true), null,
            new PersistentTasksService.WaitForPersistentTaskListener<ReindexTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexTaskParams> task) {
                    ReindexPersistentTaskState state = (ReindexPersistentTaskState) task.getState();
                    if (state.getStatus() == ReindexPersistentTaskState.Status.ASSIGNMENT_FAILED) {
                        listener.onFailure(new ElasticsearchException("Reindexing failed. Task node could not assign itself as the "
                            + "coordinating node in the " + ReindexIndexClient.REINDEX_ALIAS + " index"));
                    } else if (state.getStatus() == ReindexPersistentTaskState.Status.DONE) {
                        reindexIndexClient.getReindexTaskDoc(taskId, new ActionListener<>() {
                            @Override
                            public void onResponse(ReindexTaskState taskState) {
                                ReindexTaskStateDoc reindexState = taskState.getStateDoc();
                                if (reindexState.getException() == null) {
                                    listener.onResponse(new StartReindexTaskAction.Response(taskId, reindexState.getReindexResponse()));
                                } else {
                                    Exception exception = reindexState.getException();
                                    RestStatus statusCode = reindexState.getFailureStatusCode();
                                    listener.onFailure(new ElasticsearchStatusException(exception.getMessage(), statusCode, exception));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
                    } else {
                        throw new AssertionError("Unexpected reindex job status: " + state.getStatus());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void waitForReindexTask(String taskId, ActionListener<StartReindexTaskAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexPredicate(false), null,
            new PersistentTasksService.WaitForPersistentTaskListener<ReindexTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexTaskParams> task) {
                    ReindexPersistentTaskState state = (ReindexPersistentTaskState) task.getState();
                    listener.onResponse(new StartReindexTaskAction.Response(state.getEphemeralTaskId().toString()));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private static class ReindexPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        private boolean waitForDone;

        private ReindexPredicate(boolean waitForDone) {
            this.waitForDone = waitForDone;
        }

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }
            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();
            if (assignment == null || assignment.isAssigned() == false) {
                return false;
            }

            ReindexPersistentTaskState state = (ReindexPersistentTaskState) persistentTask.getState();


            if (waitForDone == false) {
                return isStarted(state);
            } else {
                return isDone(state);
            }
        }

        private boolean isStarted(ReindexPersistentTaskState state) {
            return state != null;
        }

        private boolean isDone(ReindexPersistentTaskState state) {
            return state != null && state.isDone();
        }
    }

    /**
     * Resolve index patterns to ensure they do not start resolving differently during reindex failovers.
     * Do not resolve aliases, since accessing the underlying indices is not semantically equivalent to accessing the alias.
     * Within each index pattern, sort the resolved indices by create date, since this ensures that if we reindex from a pattern of indices,
     * destination will receive oldest data first. This is in particular important if destination does rollover and it is time-based data.
     *
     * @return list of groups of indices/aliases that must be searched together.
     */
    private List<Set<String>> resolveIndexPatterns(ReindexRequest request) {
        return resolveIndexPatterns(request, clusterService.state(), indexNameExpressionResolver);
    }

    // visible for testing
    static List<Set<String>> resolveIndexPatterns(ReindexRequest request, ClusterState clusterState,
                                                          IndexNameExpressionResolver indexNameResolver) {
        if (request.getRemoteInfo() == null) {
            return resolveIndexPatterns(request.getSearchRequest().indices(), clusterState, indexNameResolver);
        } else {
            return Collections.emptyList();
        }
    }

    private static List<Set<String>> resolveIndexPatterns(String[] indices, ClusterState clusterState,
                                                    IndexNameExpressionResolver indexNameResolver) {
        Set<String> resolvedNames = indexNameResolver.resolveExpressions(clusterState, indices);

        List<IndexGroup> groups = Arrays.stream(indices)
            .flatMap(expression -> resolveSingleIndexExpression(expression, resolvedNames::contains,clusterState, indexNameResolver))
            .collect(Collectors.toList());

        return resolveGroups(groups).stream().map(IndexGroup::newResolvedGroup).collect(Collectors.toList());
    }

    private static List<IndexGroup> resolveGroups(List<IndexGroup> groups) {
        List<IndexGroup> result = new ArrayList<>(groups);

        // n^2, but OK since data volume is low.
        // reverse order since we bubble data towards the lower index end.
        for (int i = result.size() - 1; i >= 0; --i) {
            IndexGroup current = result.get(i);
            for (int j = i - 1; current != null && j >= 0; --j) {
                IndexGroup earlier = result.get(j);
                Tuple<IndexGroup, IndexGroup> collapsed = earlier.collapse(current);
                result.set(j, collapsed.v1());
                current = collapsed.v2();
            }
            result.set(i, current);
        }

        return result.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    private static Stream<IndexGroup> resolveSingleIndexExpression(String expression, Predicate<String> predicate,
                                                            ClusterState clusterState,
                                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        SortedMap<String, AliasOrIndex> lookup = clusterState.getMetaData().getAliasAndIndexLookup();
        Comparator<AliasOrIndex> createDateIndexOrder = (i1, i2) -> {
            if (i1.isAlias() && i2.isAlias()) {
                return ((AliasOrIndex.Alias) i1).getAliasName().compareTo(((AliasOrIndex.Alias) i2).getAliasName());
            }
            if (i1.isAlias() != i2.isAlias()) {
                return Boolean.compare(i1.isAlias(), i2.isAlias());
            }

            assert i1.getIndices().size() == 1;
            assert i2.getIndices().size() == 1;
            IndexMetaData indexMetaData1 = i1.getIndices().get(0);
            IndexMetaData indexMetaData2 = i2.getIndices().get(0);
            int compare = Long.compare(indexMetaData1.getCreationDate(), indexMetaData2.getCreationDate());
            return compare != 0 ? compare : indexMetaData1.getIndex().getName().compareTo(indexMetaData2.getIndex().getName());
        };

        return indexNameExpressionResolver.resolveExpressions(clusterState, expression).stream()
            .filter(predicate).map(lookup::get).sorted(createDateIndexOrder).map(IndexGroup::create);
    }

    /**
     * Immutable group of indices and aliases.
     */
    private static class IndexGroup {
        private final Set<String> indices;
        private final Set<String> allIndices;
        private final Set<AliasOrIndex.Alias> aliases;
        private final List<String> orderedIndices;

        private IndexGroup(List<String> indices, Set<AliasOrIndex.Alias> aliases) {
            orderedIndices = indices;
            this.indices = indices.stream().collect(Collectors.toUnmodifiableSet());
            this.aliases = aliases;
            this.allIndices = Stream.concat(indices.stream(),
                aliases.stream().flatMap(aliasOrIndex -> aliasOrIndex.getIndices().stream())
                    .map(imd -> imd.getIndex().getName())
            ).collect(Collectors.toUnmodifiableSet());
        }

        private IndexGroup(IndexGroup group1, IndexGroup group2) {
            this(Stream.concat(group1.orderedIndices.stream(), group2.orderedIndices.stream()).collect(Collectors.toList()),
                Stream.concat(group1.aliases.stream(), group2.aliases.stream())
                    .collect(Collectors.toSet()));
        }

        public static IndexGroup create(AliasOrIndex aliasOrIndex) {
            if (aliasOrIndex.isAlias()) {
                return new IndexGroup(Collections.emptyList(), Collections.singleton((AliasOrIndex.Alias) aliasOrIndex));
            } else {
                return new IndexGroup(Collections.singletonList(aliasOrIndex.getIndices().get(0).getIndex().getName()),
                    Collections.emptySet());
            }
        }

        private Tuple<IndexGroup, IndexGroup> collapse(IndexGroup other) {
            if (other == this) {
                return Tuple.tuple(this, this);
            }

            if (aliasOverlap(this.aliases, other.allIndices) || aliasOverlap(other.aliases, this.allIndices)) {
                return Tuple.tuple(new IndexGroup(this, other), null);
            }

            Set<String> intersection = Sets.intersection(indices, other.indices);
            assert intersection.isEmpty() == false || Sets.intersection(allIndices, other.allIndices).isEmpty();
            assert intersection.isEmpty() || Sets.intersection(allIndices, other.allIndices).isEmpty() == false;
            return Tuple.tuple(this.add(intersection, orderedIndices), other.subtract(intersection));
        }

        private IndexGroup add(Set<String> intersection, List<String> order) {
            if (intersection.isEmpty()) {
                return this;
            }

            List<String> indices =
                Stream.concat(this.orderedIndices.stream(), order.stream().filter(intersection::contains)).collect(Collectors.toList());
            return new IndexGroup(indices, aliases);
        }

        private IndexGroup subtract(Set<String> intersection) {
            if (intersection.isEmpty()) {
                return this;
            }

            List<String> indices =
                this.orderedIndices.stream().filter(Predicate.not(intersection::contains)).collect(Collectors.toList());

            if (indices.isEmpty()) {
                return null;
            }
            return new IndexGroup(indices, aliases);
        }

        private static boolean aliasOverlap(Set<AliasOrIndex.Alias> aliases, Set<String> indices) {
            return aliases.stream()
                .flatMap(aliasOrIndex -> aliasOrIndex.getIndices().stream()).map(imd -> imd.getIndex().getName())
                .anyMatch(indices::contains);
        }

        public Set<String> newResolvedGroup() {
            return Stream.concat(indices.stream(), aliases.stream().map(AliasOrIndex.Alias::getAliasName)).collect(Collectors.toSet());
        }
    }
}
