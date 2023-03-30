/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Asynchronously resolves {@link LookupField} that are specified {@link DocumentField#getLookupFields()}
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 */
final class FetchLookupFieldsPhase extends SearchPhase {
    private final SearchPhaseContext context;
    private final InternalSearchResponse searchResponse;
    private final AtomicArray<SearchPhaseResult> queryResults;

    FetchLookupFieldsPhase(SearchPhaseContext context, InternalSearchResponse searchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        super("fetch_lookup_fields");
        this.context = context;
        this.searchResponse = searchResponse;
        this.queryResults = queryResults;
    }

    private record Cluster(String clusterAlias, List<SearchHit> hitsWithLookupFields, List<LookupField> lookupFields) {

    }

    private static List<Cluster> groupLookupFieldsByClusterAlias(InternalSearchResponse response) {
        final Map<String, List<SearchHit>> perClusters = new HashMap<>();
        for (SearchHit hit : response.hits.getHits()) {
            String clusterAlias = hit.getClusterAlias() != null ? hit.getClusterAlias() : RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY;
            if (hit.hasLookupFields()) {
                perClusters.computeIfAbsent(clusterAlias, k -> new ArrayList<>()).add(hit);
            }
        }
        final List<Cluster> clusters = new ArrayList<>(perClusters.size());
        for (Map.Entry<String, List<SearchHit>> e : perClusters.entrySet()) {
            final List<LookupField> lookupFields = e.getValue()
                .stream()
                .flatMap(h -> h.getDocumentFields().values().stream())
                .flatMap(doc -> doc.getLookupFields().stream())
                .distinct()
                .toList();
            clusters.add(new Cluster(e.getKey(), e.getValue(), lookupFields));
        }
        return clusters;
    }

    @Override
    public void run() {
        final List<Cluster> clusters = groupLookupFieldsByClusterAlias(searchResponse);
        if (clusters.isEmpty()) {
            context.sendSearchResponse(searchResponse, queryResults);
            return;
        }
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (Cluster cluster : clusters) {
            // Do not prepend the clusterAlias to the targetIndex if the search request is already on the remote cluster.
            final String clusterAlias = context.getRequest().getLocalClusterAlias() == null ? cluster.clusterAlias : null;
            assert Strings.isEmpty(clusterAlias) || TransportSearchAction.shouldMinimizeRoundtrips(context.getRequest()) == false
                : "lookup across clusters only if [ccs_minimize_roundtrips] is disabled";
            for (LookupField lookupField : cluster.lookupFields) {
                final SearchRequest searchRequest = lookupField.toSearchRequest(clusterAlias);
                searchRequest.setCcsMinimizeRoundtrips(false);
                multiSearchRequest.add(searchRequest);
            }
        }
        context.getSearchTransport().sendExecuteMultiSearch(multiSearchRequest, context.getTask(), new ActionListener<>() {
            @Override
            public void onResponse(MultiSearchResponse items) {
                Exception failure = null;
                int index = 0;
                for (Cluster cluster : clusters) {
                    final Map<LookupField, List<Object>> lookupResults = Maps.newMapWithExpectedSize(cluster.lookupFields.size());
                    for (LookupField lookupField : cluster.lookupFields) {
                        final MultiSearchResponse.Item item = items.getResponses()[index];
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
                        index++;
                    }
                    if (failure == null) {
                        for (SearchHit hit : cluster.hitsWithLookupFields) {
                            hit.resolveLookupFields(lookupResults);
                        }
                    }
                }
                if (failure != null) {
                    context.onPhaseFailure(FetchLookupFieldsPhase.this, "failed to fetch lookup fields", failure);
                } else {
                    context.sendSearchResponse(searchResponse, queryResults);
                }
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(FetchLookupFieldsPhase.this, "failed to fetch lookup fields", e);
            }
        });
    }
}
