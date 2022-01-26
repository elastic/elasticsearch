/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Asynchronously resolves {@link LookupField} that are specified {@link DocumentField#getLookupFields()}
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 */
public final class FetchLookupFieldsPhase {

    private final SearchTransportService searchTransportService;
    private final BooleanSupplier allowExpensiveQueries;

    public FetchLookupFieldsPhase(SearchTransportService searchTransportService, BooleanSupplier allowExpensiveQueries) {
        this.searchTransportService = searchTransportService;
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    /**
     * Asynchronously fetches the lookup fields specified in {@link DocumentField#getLookupFields()} of the given search hits.
     *
     * @param searchTask   the parent task of the search request
     * @param clusterAlias the cluster alias of the search request - lookup fields are resolved in only the local cluster
     * @param hits         the search hits
     * @param listener     the listener to be called when the lookup completes
     */
    public void fetchLookupFields(SearchTask searchTask, String clusterAlias, SearchHits hits, ActionListener<Void> listener) {
        // Do not fetch lookup fields on remote clusters
        if (clusterAlias != null && RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
            listener.onResponse(null);
            return;
        }
        final LookupField[] lookupFields = Arrays.stream(hits.getHits())
            .flatMap(hit -> hit.getDocumentFields().values().stream())
            .flatMap(docField -> docField.getLookupFields().stream())
            .distinct()
            .toArray(LookupField[]::new);
        if (lookupFields.length == 0) {
            listener.onResponse(null);
            return;
        }
        if (allowExpensiveQueries.getAsBoolean() == false) {
            listener.onFailure(
                new ElasticsearchException(
                    "cannot be executed against lookup field while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
                )
            );
            return;
        }
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (LookupField lookupField : lookupFields) {
            multiSearchRequest.add(lookupField.toSearchRequest());
        }
        searchTransportService.sendExecuteMultiSearch(multiSearchRequest, searchTask, listener.delegateFailure((innerListener, items) -> {
            final Map<LookupField, List<Object>> lookupResults = new HashMap<>();
            Exception failure = null;
            for (int i = 0; i < items.getResponses().length; i++) {
                final MultiSearchResponse.Item item = items.getResponses()[i];
                final LookupField lookupField = lookupFields[i];
                if (item.isFailure()) {
                    failure = ExceptionsHelper.useOrSuppress(failure, item.getFailure());
                } else if (failure == null) {
                    final List<Object> fetchedValues = new ArrayList<>();
                    for (SearchHit rightHit : item.getResponse().getHits()) {
                        final Map<String, List<Object>> fetchedFields = rightHit.getDocumentFields()
                            .values()
                            .stream()
                            .collect(Collectors.toMap(DocumentField::getName, DocumentField::getValues));
                        if (fetchedFields.isEmpty() == false) {
                            fetchedValues.add(fetchedFields);
                        }
                    }
                    lookupResults.put(lookupField, fetchedValues);
                }
            }
            if (failure != null) {
                innerListener.onFailure(failure);
            } else {
                for (SearchHit hit : hits.getHits()) {
                    hit.resolveLookupFields(lookupResults);
                }
                innerListener.onResponse(null);
            }
        }));
    }
}
