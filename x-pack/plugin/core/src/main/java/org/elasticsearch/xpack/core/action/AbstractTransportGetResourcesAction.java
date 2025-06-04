/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.action.util.QueryPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Abstract transport class for collecting common logic in gathering Resource objects from indices
 * @param <Resource> The type of the Resource being gathered
 * @param <Request> The type of the Request
 * @param <Response> The type of the Response
 */
public abstract class AbstractTransportGetResourcesAction<
    Resource extends ToXContent & Writeable,
    Request extends AbstractGetResourcesRequest,
    Response extends AbstractGetResourcesResponse<Resource>> extends HandledTransportAction<Request, Response> {

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    protected AbstractTransportGetResourcesAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(actionName, transportService, actionFilters, request, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = Objects.requireNonNull(client);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
    }

    protected void searchResources(AbstractGetResourcesRequest request, TaskId parentTaskId, ActionListener<QueryPage<Resource>> listener) {
        String[] tokens = Strings.tokenizeToStringArray(request.getResourceId(), ",");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().sort(
            SortBuilders.fieldSort(request.getResourceIdField())
                // If there are no resources, there might be no mapping for the id field.
                // This makes sure we don't get an error if that happens.
                .unmappedType("long")
        ).query(buildQuery(tokens, request.getResourceIdField()));
        if (request.getPageParams() != null) {
            sourceBuilder.from(request.getPageParams().getFrom()).size(request.getPageParams().getSize());
        }
        sourceBuilder.trackTotalHits(true);

        IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
        SearchRequest searchRequest = new SearchRequest(getIndices()).indicesOptions(
            IndicesOptions.fromOptions(
                true,
                indicesOptions.allowNoIndices(),
                indicesOptions.expandWildcardsOpen(),
                indicesOptions.expandWildcardsClosed(),
                indicesOptions
            )
        ).source(customSearchOptions(sourceBuilder));
        searchRequest.setParentTask(parentTaskId);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            executionOrigin(),
            searchRequest,
            listener.<SearchResponse>delegateFailure((l, response) -> {
                List<Resource> docs = new ArrayList<>();
                Set<String> foundResourceIds = new HashSet<>();
                long totalHitCount = response.getHits().getTotalHits().value();
                for (SearchHit hit : response.getHits().getHits()) {
                    try (
                        XContentParser parser = XContentHelper.createParserNotCompressed(
                            LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG.withRegistry(xContentRegistry),
                            hit.getSourceRef(),
                            XContentType.JSON
                        )
                    ) {
                        Resource resource = parse(parser);
                        String id = extractIdFromResource(resource);
                        // Do not include a resource with the same ID twice
                        if (foundResourceIds.contains(id) == false) {
                            docs.add(resource);
                            foundResourceIds.add(id);
                        }
                    } catch (IOException e) {
                        l.onFailure(e);
                    }
                }
                ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, request.isAllowNoResources());
                requiredMatches.filterMatchedIds(foundResourceIds);
                if (requiredMatches.hasUnmatchedIds()) {
                    l.onFailure(notFoundException(requiredMatches.unmatchedIdsString()));
                } else {
                    // if only exact ids have been given, take the count from docs to avoid potential duplicates
                    // in versioned indexes (like transform)
                    if (requiredMatches.isOnlyExact()) {
                        l.onResponse(new QueryPage<>(docs, docs.size(), getResultsField()));
                    } else {
                        l.onResponse(new QueryPage<>(docs, totalHitCount, getResultsField()));
                    }
                }
            }),
            client::search
        );
    }

    private QueryBuilder buildQuery(String[] tokens, String resourceIdField) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // If the resourceId is not _all or *, we should see if it is a comma delimited string with wild-cards
        // e.g. id1,id2*,id3
        if (Strings.isAllOrWildcard(tokens) == false) {
            BoolQueryBuilder shouldQueries = new BoolQueryBuilder();
            List<String> terms = new ArrayList<>();
            for (String token : tokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    shouldQueries.should(QueryBuilders.wildcardQuery(resourceIdField, token));
                } else {
                    terms.add(token);
                }
            }
            if (terms.isEmpty() == false) {
                shouldQueries.should(QueryBuilders.termsQuery(resourceIdField, terms));
            }

            if (shouldQueries.should().isEmpty() == false) {
                boolQuery.filter(shouldQueries);
            }
        }
        QueryBuilder additionalQuery = additionalQuery();
        if (additionalQuery != null) {
            boolQuery.filter(additionalQuery);
        }
        return boolQuery.hasClauses() ? boolQuery : QueryBuilders.matchAllQuery();
    }

    protected SearchSourceBuilder customSearchOptions(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder;
    }

    @Nullable
    protected QueryBuilder additionalQuery() {
        return null;
    }

    /**
     * @return The results field parse field so that the response is properly formatted
     */
    protected abstract ParseField getResultsField();

    /**
     * @return The indices needed to query
     */
    protected abstract String[] getIndices();

    /**
     * @param parser Constructed XContentParser from search response hits to relay to a parser for the Resource
     * @return parsed Resource typed object
     */
    protected abstract Resource parse(XContentParser parser) throws IOException;

    /**
     * @param resourceId Resource ID or expression that was not found in the search results
     * @return The exception to throw in the event that an ID or expression is not found
     */
    protected abstract ResourceNotFoundException notFoundException(String resourceId);

    /**
     * @return The appropriate origin under which to execute the search requests
     */
    protected abstract String executionOrigin();

    /**
     * @param resource A parsed Resource object
     * @return The ID of the resource
     */
    protected abstract String extractIdFromResource(Resource resource);
}
