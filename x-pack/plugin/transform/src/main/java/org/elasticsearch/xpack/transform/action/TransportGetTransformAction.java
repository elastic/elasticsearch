/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.transform.TransformField.INDEX_DOC_TYPE;

public class TransportGetTransformAction extends AbstractTransportGetResourcesAction<TransformConfig, Request, Response> {

    private static final String DANGLING_TASK_ERROR_MESSAGE_FORMAT =
        "Found task for transform [%s], but no configuration for it. To delete this transform use DELETE with force=true.";

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(GetTransformAction.NAME, transportService, actionFilters, Request::new, client, xContentRegistry);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final ClusterState clusterState = clusterService.state();
        TransformNodes.warnIfNoTransformNodes(clusterState);

        RemainingTime remainingTime = RemainingTime.from(Instant::now, request.timeout());

        // Step 2: Search for all the transform tasks (matching the request) that *do not* have corresponding transform config.
        ActionListener<QueryPage<TransformConfig>> searchTransformConfigsListener = listener.delegateFailureAndWrap((l, r) -> {
            if (request.checkForDanglingTasks()) {
                getAllTransformIds(request, r, remainingTime, l.delegateFailureAndWrap((ll, transformConfigIds) -> {
                    var errors = TransformTask.findTransformTasks(request.getId(), clusterState)
                        .stream()
                        .map(PersistentTasksCustomMetadata.PersistentTask::getId)
                        .filter(not(transformConfigIds::contains))
                        .map(
                            transformId -> new Response.Error(
                                "dangling_task",
                                Strings.format(DANGLING_TASK_ERROR_MESSAGE_FORMAT, transformId)
                            )
                        )
                        .toList();
                    ll.onResponse(new Response(r.results(), r.count(), errors.isEmpty() ? null : errors));
                }));
            } else {
                l.onResponse(new Response(r.results(), r.count(), null));
            }
        });

        // Step 1: Search for all the transform configs matching the request.
        searchResources(request, parentTaskId, searchTransformConfigsListener);
    }

    @Override
    protected ParseField getResultsField() {
        return TransformField.TRANSFORMS;
    }

    @Override
    protected String[] getIndices() {
        return new String[] {
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED };
    }

    @Override
    protected TransformConfig parse(XContentParser parser) {
        return TransformConfig.fromXContent(parser, null, true);
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
        RemainingTime remainingTime,
        ActionListener<Set<String>> listener
    ) {
        ActionListener<Stream<String>> transformIdListener = listener.map(stream -> stream.collect(toSet()));
        var requestedPage = initialResults.results().stream().map(TransformConfig::getId);

        if (initialResults.count() == initialResults.results().size()) {
            transformIdListener.onResponse(requestedPage);
        } else {
            // if we do not have all of our transform ids already, we have to go get them
            // we'll read everything after our current page, then we'll reverse and read everything before our current page
            var from = request.getPageParams().getFrom();
            var size = request.getPageParams().getSize();
            var idTokens = ExpandedIdsMatcher.tokenizeExpression(request.getResourceId());

            getAllTransformIds(idTokens, false, from, size, remainingTime, transformIdListener.delegateFailureAndWrap((l, nextPages) -> {
                var currentPages = Stream.concat(requestedPage, nextPages);
                getAllTransformIds(idTokens, true, from, size, remainingTime, l.map(firstPages -> Stream.concat(firstPages, currentPages)));
            }));
        }
    }

    private void getAllTransformIds(
        String[] idTokens,
        boolean reverse,
        int from,
        int size,
        RemainingTime remainingTime,
        ActionListener<Stream<String>> listener
    ) {
        if (reverse && from <= 0) {
            listener.onResponse(Stream.empty());
            return;
        }

        var thisPage = reverse ? from - size : from + size;
        var thisPageFrom = Math.max(0, thisPage);
        var thisPageSize = thisPage < 0 ? from : size;

        SearchRequest request = client.prepareSearch(
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        )
            .addSort(TransformField.ID.getPreferredName(), SortOrder.ASC)
            .addSort("_index", SortOrder.DESC)
            .setFrom(thisPageFrom)
            .setSize(thisPageSize)
            .setTimeout(remainingTime.get())
            .setFetchSource(false)
            .setTrackTotalHits(true)
            .addDocValueField(TransformField.ID.getPreferredName())
            .setQuery(query(idTokens))
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            TRANSFORM_ORIGIN,
            request,
            listener.<SearchResponse>delegateFailureAndWrap((l, searchResponse) -> {
                var transformIds = Arrays.stream(searchResponse.getHits().getHits())
                    .map(hit -> (String) hit.field(TransformField.ID.getPreferredName()).getValue())
                    .filter(Predicate.not(org.elasticsearch.common.Strings::isNullOrEmpty))
                    .toList()
                    .stream();

                if (searchResponse.getHits().getHits().length == size) {
                    getAllTransformIds(
                        idTokens,
                        reverse,
                        thisPageFrom,
                        thisPageSize,
                        remainingTime,
                        l.map(nextTransformIds -> Stream.concat(transformIds, nextTransformIds))
                    );
                } else {
                    l.onResponse(transformIds);
                }
            }),
            client::search
        );
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
