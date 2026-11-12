/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.common.time.RemainingTime;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformParsingContext;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.transform.TransformField.INDEX_DOC_TYPE;

public class TransportGetTransformAction extends AbstractTransportGetResourcesAction<TransformConfig, Request, Response> {

    static final int TRANSFORM_ID_PAGE_SIZE = 1_000;

    private static final Logger logger = LogManager.getLogger(TransportGetTransformAction.class);

    private static final String DANGLING_TASK_ERROR_MESSAGE_FORMAT =
        "Found task for transform [%s], but no configuration for it. To delete this transform use DELETE with force=true.";

    private final ClusterService clusterService;
    private final Client client;
    private final TransformParsingContext transformParsingContext;
    private final ProjectResolver projectResolver;
    /**
     * Returns {@code true} when CPS is enabled, the transform cross-project feature flag is on,
     * and the current project has at least one linked project. Used to determine whether to display
     * a default {@code project_routing} of {@link ProjectRoutingResolver#LOCAL_ONLY} for unmigrated
     * transforms on GET.
     */
    private final BooleanSupplier shouldDisplayLocalOnlyDefault;

    @Inject
    public TransportGetTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        TransformServices transformServices,
        ProjectResolver projectResolver
    ) {
        super(GetTransformAction.NAME, transportService, actionFilters, Request::new, client, xContentRegistry);
        this.clusterService = clusterService;
        this.client = client;
        boolean crossProjectEnabled = transformServices.crossProjectModeDecider().crossProjectEnabled()
            && TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled();
        this.transformParsingContext = new TransformParsingContext(crossProjectEnabled);
        this.projectResolver = projectResolver;
        this.shouldDisplayLocalOnlyDefault = crossProjectEnabled
            ? () -> transformServices.hasLinkedProjects().apply(projectResolver.getProjectId())
            : () -> false;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final ClusterState clusterState = clusterService.state();
        TransformNodes.warnIfNoTransformNodes(projectResolver.getProjectMetadata(clusterState), clusterState.getNodes());

        RemainingTime remainingTime = RemainingTime.from(Instant::now, request.timeout());

        // When CPS is enabled and there are linked projects, unmigrated transforms (no credentialId, no explicit
        // project_routing) display a default of LOCAL_ONLY so users understand their effective search scope.
        // This is a display-only default: it is never persisted. Once a transform is migrated to a UIAM token
        // the default is persisted by TransformUpdater, so this code path only applies to legacy/API-key transforms.
        final boolean applyDisplayDefault = shouldDisplayLocalOnlyDefault.getAsBoolean();

        // Step 2: Search for all the transform tasks (matching the request) that *do not* have corresponding transform config.
        ActionListener<QueryPage<TransformConfig>> searchTransformConfigsListener = listener.delegateFailureAndWrap((l, r) -> {
            List<TransformConfig> configs = applyDisplayDefault ? withLocalOnlyDisplayDefault(r.results()) : r.results();
            if (request.checkForDanglingTasks()) {
                var configIdsInPage = r.results().stream().map(TransformConfig::getId).collect(toSet());
                var unverifiedTaskIds = TransformTask.findTransformTasks(request.getId(), projectResolver.getProjectMetadata(clusterState))
                    .stream()
                    .map(PersistentTasksCustomMetadata.PersistentTask::getId)
                    .filter(not(configIdsInPage::contains))
                    .collect(toSet());
                if (unverifiedTaskIds.isEmpty()) {
                    // every matching task has its config in the requested page, so no task can be dangling
                    l.onResponse(new Response(configs, r.count(), null));
                    return;
                }
                // AbstractGetResourcesRequest#createTask always creates a CancellableTask
                var cancellableTask = (CancellableTask) task;
                getAllTransformIds(
                    request,
                    r,
                    cancellableTask,
                    parentTaskId,
                    request.timeout(),
                    remainingTime,
                    respondWithDanglingTasks(request.getId(), cancellableTask, unverifiedTaskIds, configs, r.count(), l)
                );
            } else {
                l.onResponse(new Response(configs, r.count(), null));
            }
        });

        // Step 1: Search for all the transform configs matching the request.
        searchResources(request, parentTaskId, searchTransformConfigsListener);
    }

    /**
     * Responds with the dangling tasks once the transform config ids are collected. Only a failure to collect them reaches
     * {@link #respondWithoutDanglingTaskCheckOnFailure}: {@code map} does not pass an exception thrown by the downstream listener to
     * {@code onFailure}, so it cannot turn into a second response.
     */
    static ActionListener<Set<String>> respondWithDanglingTasks(
        String idExpression,
        CancellableTask task,
        Set<String> unverifiedTaskIds,
        List<TransformConfig> configs,
        long count,
        ActionListener<Response> listener
    ) {
        return respondWithoutDanglingTaskCheckOnFailure(idExpression, task, new Response(configs, count, null), listener).map(
            transformConfigIds -> {
                var errors = unverifiedTaskIds.stream()
                    .filter(not(transformConfigIds::contains))
                    .sorted()
                    .map(
                        transformId -> new Response.Error("dangling_task", Strings.format(DANGLING_TASK_ERROR_MESSAGE_FORMAT, transformId))
                    )
                    .toList();
                return new Response(configs, count, errors.isEmpty() ? null : errors);
            }
        );
    }

    /**
     * The dangling task check is diagnostic, so when it fails (for example by timing out or hitting unavailable shards) the configs are
     * still returned, with a warning header in place of the check's result and the cause logged. Cancellation still fails the request.
     */
    static ActionListener<Response> respondWithoutDanglingTaskCheckOnFailure(
        String idExpression,
        CancellableTask task,
        Response withoutDanglingTaskCheck,
        ActionListener<Response> listener
    ) {
        return listener.delegateResponse((l, e) -> {
            if (task.isCancelled()) {
                l.onFailure(e);
                return;
            }
            // the warning header already tells the caller, and GET is polled, so a failed check is not worth more than a debug log
            logger.debug(() -> Strings.format("Failed to check for dangling tasks of transforms matching [%s]", idExpression), e);
            // with partial results disallowed, a shard timeout fails the search with a SearchTimeoutException
            if (ExceptionsHelper.unwrap(e, ElasticsearchTimeoutException.class, SearchTimeoutException.class) != null) {
                HeaderWarning.addWarning(TransformMessages.REST_WARN_DANGLING_TASK_CHECK_TIMED_OUT);
            } else {
                HeaderWarning.addWarning(TransformMessages.REST_WARN_DANGLING_TASK_CHECK_FAILED);
            }
            l.onResponse(withoutDanglingTaskCheck);
        });
    }

    @Override
    protected ParseField getResultsField() {
        return TransformField.TRANSFORMS;
    }

    /**
     * For each config in {@code configs}, if the transform has not yet been migrated to a UIAM
     * credential (no {@code credentialId}) and has no explicit {@code project_routing}, returns a
     * copy with {@code project_routing} defaulted to {@link ProjectRoutingResolver#LOCAL_ONLY}.
     * This is a display-only operation: the returned configs are never persisted.
     */
    private static List<TransformConfig> withLocalOnlyDisplayDefault(List<TransformConfig> configs) {
        return configs.stream().map(config -> {
            if (config.getCredentialId() == null && config.getSource().getProjectRouting() == null) {
                return new TransformConfig.Builder(config).setSource(
                    config.getSource().withProjectRouting(ProjectRoutingResolver.LOCAL_ONLY)
                ).build();
            }
            return config;
        }).toList();
    }

    @Override
    protected String[] getIndices() {
        return new String[] {
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED };
    }

    @Override
    protected TransformConfig parse(XContentParser parser) {
        return TransformConfig.fromXContent(parser, null, true, transformParsingContext);
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, resourceId));
    }

    @Override
    protected String executionOrigin() {
        return TRANSFORM_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(TransformConfig transformConfig) {
        return transformConfig.getId();
    }

    @Override
    protected QueryBuilder additionalQuery() {
        return QueryBuilders.termQuery(INDEX_DOC_TYPE.getPreferredName(), TransformConfig.NAME);
    }

    @Override
    protected SearchSourceBuilder customSearchOptions(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.sort("_index", SortOrder.DESC).sort(TransformField.ID.getPreferredName(), SortOrder.ASC);
    }

    private void getAllTransformIds(
        Request request,
        QueryPage<TransformConfig> initialResults,
        CancellableTask task,
        TaskId parentTaskId,
        TimeValue timeout,
        RemainingTime remainingTime,
        ActionListener<Set<String>> listener
    ) {
        if (initialResults.count() == initialResults.results().size()) {
            listener.onResponse(initialResults.results().stream().map(TransformConfig::getId).collect(toSet()));
        } else {
            // The requested page does not hold every matching transform, so page through all of their ids.
            // The page size is fixed rather than taken from the request, so each page makes progress even when the request's size is 0.
            new TransformIdCollector(
                client,
                ExpandedIdsMatcher.tokenizeExpression(request.getResourceId()),
                task,
                parentTaskId,
                timeout,
                remainingTime
            ).collect(listener);
        }
    }

    /**
     * Pages through the ids of every transform config matching the request. Ids accumulate in a single set and every page completes
     * the same listener, so memory and listener depth stay bounded regardless of the number of pages.
     */
    static class TransformIdCollector {
        private final Client client;
        private final String[] idTokens;
        private final CancellableTask task;
        private final TaskId parentTaskId;
        private final TimeValue timeout;
        private final RemainingTime remainingTime;

        TransformIdCollector(
            Client client,
            String[] idTokens,
            CancellableTask task,
            TaskId parentTaskId,
            TimeValue timeout,
            RemainingTime remainingTime
        ) {
            this.client = client;
            this.idTokens = idTokens;
            this.task = task;
            this.parentTaskId = parentTaskId;
            this.timeout = timeout;
            this.remainingTime = remainingTime;
        }

        void collect(ActionListener<Set<String>> listener) {
            collectPage(null, new HashSet<>(), listener);
        }

        /**
         * Pages with search_after rather than from, so collection is not capped by index.max_result_window.
         * Sorting by id alone is enough: the only id that can appear twice is a config present in both the current and the deprecated
         * internal index, and skipping its second copy at a page boundary loses nothing because the ids are collected into a set.
         */
        private void collectPage(@Nullable Object[] searchAfter, Set<String> transformIds, ActionListener<Set<String>> listener) {
            if (task.notifyIfCancelled(listener)) {
                return;
            }
            var pageTimeout = remainingTime.get();
            if (pageTimeout.nanos() <= 0) {
                listener.onFailure(timeoutException(transformIds));
                return;
            }

            var requestBuilder = client.prepareSearch(
                TransformInternalIndexConstants.INDEX_NAME_PATTERN,
                TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
            )
                // an internal index without an id mapping must not fail the search, now that partial results are disallowed
                .addSort(SortBuilders.fieldSort(TransformField.ID.getPreferredName()).order(SortOrder.ASC).unmappedType("keyword"))
                .setSize(TRANSFORM_ID_PAGE_SIZE)
                .setTimeout(pageTimeout)
                .setFetchSource(false)
                .setTrackTotalHits(false)
                .addDocValueField(TransformField.ID.getPreferredName())
                .setQuery(query(idTokens))
                .setIndicesOptions(LENIENT_INDICES_OPTIONS)
                // a partial page would look like the last page and leave transforms out of the ids, reporting them as dangling
                .setAllowPartialSearchResults(false);
            if (searchAfter != null) {
                requestBuilder.searchAfter(searchAfter);
            }
            SearchRequest searchRequest = requestBuilder.request();
            searchRequest.setParentTask(parentTaskId);

            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                TRANSFORM_ORIGIN,
                searchRequest,
                // not delegateFailureAndWrap: an exception from completing the listener must not reach onFailure, which responds again
                listener.<SearchResponse>delegateFailure((l, searchResponse) -> {
                    var hits = searchResponse.getHits().getHits();
                    try {
                        for (var hit : hits) {
                            DocumentField idField = hit.field(TransformField.ID.getPreferredName());
                            String transformId = idField == null ? null : idField.getValue();
                            if (org.elasticsearch.common.Strings.isNullOrEmpty(transformId) == false) {
                                transformIds.add(transformId);
                            }
                        }
                    } catch (RuntimeException e) {
                        l.onFailure(e);
                        return;
                    }

                    if (hits.length == TRANSFORM_ID_PAGE_SIZE) {
                        collectPage(hits[hits.length - 1].getSortValues(), transformIds, l);
                    } else {
                        l.onResponse(transformIds);
                    }
                }),
                client::search
            );
        }

        private ElasticsearchTimeoutException timeoutException(Set<String> transformIds) {
            return new ElasticsearchTimeoutException(
                "Timed out after [{}] while collecting transform ids to check for dangling tasks, collected [{}] ids so far",
                timeout,
                transformIds.size()
            );
        }
    }

    private static QueryBuilder query(String[] idTokens) {
        var queryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(TransformField.INDEX_DOC_TYPE.getPreferredName(), TransformConfig.NAME));

        if (org.elasticsearch.common.Strings.isAllOrWildcard(idTokens) == false) {
            var shouldQueries = new BoolQueryBuilder();
            var terms = new ArrayList<String>();
            for (String token : idTokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    shouldQueries.should(QueryBuilders.wildcardQuery(TransformField.ID.getPreferredName(), token));
                } else {
                    terms.add(token);
                }
            }

            if (terms.isEmpty() == false) {
                shouldQueries.should(QueryBuilders.termsQuery(TransformField.ID.getPreferredName(), terms));
            }

            if (shouldQueries.should().isEmpty() == false) {
                queryBuilder.filter(shouldQueries);
            }
        }

        return QueryBuilders.constantScoreQuery(queryBuilder);
    }

}
